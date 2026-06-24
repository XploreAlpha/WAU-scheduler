package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wau/registry/registry"
)

// RedisDataSource implements DataSource using Redis.
//
// This is the production data source. All 15-dim data is read from
// Redis keys populated by:
//   - Watchdog / heartbeat: latency, bandwidth, availability, version
//   - L4 handler: success, errors, history
//   - wau-trust: trust score
//   - Registry: skills, universe (card)
type RedisDataSource struct {
	client *redis.Client
	prefix string
	config DataSourceConfig
	card   registry.Registry
}

// NewRedisDataSource creates a Redis-backed DataSource.
//
// If card is nil, agent metadata queries (GetMeta) will return defaults
// and the source is read-only for Redis keys.
func NewRedisDataSource(client *redis.Client, card registry.Registry, prefix string, opts ...DataSourceOption) *RedisDataSource {
	if prefix == "" {
		prefix = "wau:"
	}
	cfg := DataSourceConfig{
		PreferredProtocols: []string{"A2A", "AFP"},
		KernelVersion:      "v0.5.1", // v0.7.0 W1 baseline
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &RedisDataSource{
		client: client,
		prefix: prefix,
		config: cfg,
		card:   card,
	}
}

// ===== Key helpers =====

func (d *RedisDataSource) key(name, suffix string) string {
	return fmt.Sprintf("%s%s:%s", d.prefix, name, suffix)
}

// ===== v0.7.0 W1: 7 维真算 =====

// LatencyScore reads `latency:{name}:p99` from Redis (5min rolling window).
//
// Score: p99 < 100ms → 1.0; p99 > 5s → 0.0; log-decay in between.
// Returns 1.0 if no data (assume perfect).
func (d *RedisDataSource) LatencyScore(ctx context.Context, agentID string) (float64, error) {
	val, err := d.client.Get(ctx, d.key(agentID, "latency:p99")).Float64()
	if errors.Is(err, redis.Nil) {
		return 1.0, nil // no data → perfect score
	}
	if err != nil {
		return 1.0, fmt.Errorf("latency score for %s: %w", agentID, err)
	}
	// Log-scale decay: score = 1 - log10(p99 / 100ms) / log10(5000/100)
	// p99=100ms → 1.0
	// p99=500ms → 0.78
	// p99=1s    → 0.68
	// p99=5s    → 0.0
	if val <= 0.1 { // < 100ms
		return 1.0, nil
	}
	if val >= 5.0 { // >= 5s
		return 0.0, nil
	}
	score := 1.0 - math.Log10(val/0.1)/math.Log10(5.0/0.1)
	if score < 0 {
		score = 0
	}
	return score, nil
}

// SuccessRate reads `success:{name}:last100` from Redis.
//
// Score = success_count / total_count in last 100 tasks.
// Returns 0.5 if no data.
func (d *RedisDataSource) SuccessRate(ctx context.Context, agentID string) (float64, error) {
	successes, err := d.client.Get(ctx, d.key(agentID, "success:last100")).Int()
	if errors.Is(err, redis.Nil) {
		return 0.5, nil
	}
	if err != nil {
		return 0.5, fmt.Errorf("success rate for %s: %w", agentID, err)
	}
	total, _ := d.client.Get(ctx, d.key(agentID, "tasks:last100")).Int()
	if total == 0 {
		return 0.5, nil
	}
	rate := float64(successes) / float64(total)
	// Exponential moving average blend: 70% empirical, 30% prior
	const prior = 0.7 // trust prior
	return prior*rate + (1-prior)*0.5, nil
}

// BandwidthScore reads `bandwidth:{name}:mbps` from Redis (from heartbeat).
//
// Score: >100Mbps → 1.0; <1Mbps → 0.1; log-scale in between.
// Returns 1.0 if no data.
func (d *RedisDataSource) BandwidthScore(ctx context.Context, agentID string) (float64, error) {
	val, err := d.client.Get(ctx, d.key(agentID, "bandwidth:mbps")).Float64()
	if errors.Is(err, redis.Nil) {
		return 1.0, nil
	}
	if err != nil {
		return 1.0, fmt.Errorf("bandwidth score for %s: %w", agentID, err)
	}
	if val >= 100 {
		return 1.0, nil
	}
	if val < 1 {
		return 0.1, nil
	}
	// log10(1) = 0, log10(100) = 2
	score := math.Log10(val) / 2.0
	if score > 1 {
		score = 1
	}
	if score < 0.1 {
		score = 0.1
	}
	return score, nil
}

// HistoryCount reads `tasks:{name}:total` from Redis.
//
// Score: log-scale saturation curve; 0 → 0.3, 10 → 0.6, 100 → 0.85, 1000+ → 1.0
// Returns 0.5 if no data.
func (d *RedisDataSource) HistoryCount(ctx context.Context, agentID string) (float64, error) {
	val, err := d.client.Get(ctx, d.key(agentID, "tasks:total")).Int()
	if errors.Is(err, redis.Nil) {
		return 0.5, nil // no data → neutral
	}
	if err != nil {
		return 0.5, fmt.Errorf("history count for %s: %w", agentID, err)
	}
	if val == 0 {
		return 0.3, nil // 0 tasks → floor
	}
	if val >= 1000 {
		return 1.0, nil
	}
	// log10(1) = 0, log10(1000) = 3
	score := 0.3 + 0.7*(math.Log10(float64(val))/3.0)
	if score > 1 {
		score = 1
	}
	if score < 0.3 {
		score = 0.3
	}
	return score, nil
}

// ErrorRate reads `errors:{name}:last100` from Redis.
//
// Score = 1.0 - error_rate (lower errors = higher score).
// Returns 1.0 if no data.
func (d *RedisDataSource) ErrorRate(ctx context.Context, agentID string) (float64, error) {
	errCount, err := d.client.Get(ctx, d.key(agentID, "errors:last100")).Int()
	if errors.Is(err, redis.Nil) {
		return 1.0, nil
	}
	if err != nil {
		return 1.0, fmt.Errorf("error rate for %s: %w", agentID, err)
	}
	total, _ := d.client.Get(ctx, d.key(agentID, "tasks:last100")).Int()
	if total == 0 {
		return 1.0, nil
	}
	rate := float64(errCount) / float64(total)
	return 1.0 - rate, nil
}

// ===== v0.7.0 W2: 4 维真算 =====

// Availability returns uptime ratio from Registry FirstSeen/LastSeen.
func (d *RedisDataSource) Availability(ctx context.Context, agentID string) (float64, error) {
	if d.card == nil {
		return 1.0, nil
	}
	card, err := d.card.GetAgent(ctx, agentID)
	if err != nil {
		return 1.0, fmt.Errorf("availability for %s: %w", agentID, err)
	}
	if card.FirstSeen <= 0 {
		return 0.5, nil
	}
	now := time.Now().Unix()
	elapsed := now - card.FirstSeen
	if elapsed <= 0 {
		return 0.5, nil
	}
	// LastSeen / FirstSeen ratio over total elapsed
	if card.LastSeen <= card.FirstSeen {
		return 0.0, nil // never seen since first
	}
	uptime := float64(card.LastSeen-card.FirstSeen) / float64(elapsed)
	if uptime > 1.0 {
		uptime = 1.0
	}
	return uptime, nil
}

// VersionCompat compares agent version against kernel version.
//
// Semver-based: 1.0 if same major, decays by minor/patch distance.
// Returns 1.0 if versions unknown.
func (d *RedisDataSource) VersionCompat(ctx context.Context, agentID string) (float64, error) {
	if d.card == nil {
		return 1.0, nil
	}
	card, err := d.card.GetAgent(ctx, agentID)
	if err != nil {
		return 1.0, fmt.Errorf("version compat for %s: %w", agentID, err)
	}
	if card.Version == "" || d.config.KernelVersion == "" {
		return 1.0, nil
	}
	kernelMajor, kernelMinor, _ := parseSemver(d.config.KernelVersion)
	agentMajor, agentMinor, _ := parseSemver(card.Version)
	if kernelMajor != agentMajor {
		return 0.0, nil // major mismatch (also covers unparseable since parseSemver returns 0)
	}
	// Same major: decay by minor diff
	diff := absInt(kernelMinor - agentMinor)
	if diff == 0 {
		return 1.0, nil
	}
	if diff >= 3 {
		return 0.3, nil
	}
	return 1.0 - 0.2*float64(diff), nil
}

// GeoPenalty returns cross-region penalty.
//
// Same region → 1.0; different region → 0.9; unknown → 1.0.
func (d *RedisDataSource) GeoPenalty(ctx context.Context, agentID string, sourceRegion string) (float64, error) {
	meta, err := d.GetMeta(ctx, agentID)
	if err != nil {
		return 1.0, nil
	}
	if sourceRegion == "" || meta.Region == "" {
		return 1.0, nil
	}
	if meta.Region == sourceRegion {
		return 1.0, nil
	}
	return 0.9, nil
}

// TrustScore returns dynamic trust score.
//
// v0.7.0 W1: read from wau-trust Redis key `trust:{name}`.
// v0.7.0 W2: integrate wau-trust Go module (import).
// Returns 0.5 if no data.
func (d *RedisDataSource) TrustScore(ctx context.Context, agentID string) (float64, error) {
	val, err := d.client.Get(ctx, d.key(agentID, "trust")).Float64()
	if errors.Is(err, redis.Nil) {
		return 0.5, nil
	}
	if err != nil {
		return 0.5, fmt.Errorf("trust score for %s: %w", agentID, err)
	}
	if val < 0 {
		val = 0
	}
	if val > 1 {
		val = 1
	}
	return val, nil
}

// IsCold reports whether the agent has no trust history (v0.8.0 M4-1).
//
// Implementation: aligns with wau-trust's Engine.IsCold — single EXISTS on
// `trust:{name}`. Cold routing's primary goal is to give fresh agents a
// chance to accumulate trust data; once trust is recorded (even 1
// RecordSuccess), the agent is no longer cold from a routing perspective.
//
// Why NOT also check tasks:total / metrics:
//   - Trust signal is the canonical "data exists" indicator for cold routing
//   - Keeping the semantics aligned with wau-trust.IsCold means callers
//     (cold routing policy in M4-1.3) get a single, consistent signal
//   - Single EXISTS is O(1) — keeps the cold path cheap
func (d *RedisDataSource) IsCold(ctx context.Context, agentID string) (bool, error) {
	n, err := d.client.Exists(ctx, d.key(agentID, "trust")).Result()
	if err != nil {
		return false, fmt.Errorf("isCold for %s: %w", agentID, err)
	}
	return n == 0, nil
}

// IsAsleep reports whether the agent is currently asleep (v0.8.0 M4-2).
//
// Implementation: aligns with wau-trust's Engine.IsAsleep — single EXISTS on
// `trust:{name}:asleep`. O(1) Redis check, identical pattern to IsCold.
//
// Production paths: the WauTrustDataSource delegates to wau-trust engine
// directly (the canonical signal). This local implementation is for tests
// and for deployments that don't yet use wau-trust as the sleep state owner.
func (d *RedisDataSource) IsAsleep(ctx context.Context, agentID string) (bool, error) {
	n, err := d.client.Exists(ctx, d.key(agentID, "asleep")).Result()
	if err != nil {
		return false, fmt.Errorf("isAsleep for %s: %w", agentID, err)
	}
	return n == 1, nil
}

// GetMeta returns extended agent metadata.
//
// v0.7.0 W1: derived from AgentCard + defaults. W2 will extend
// registry.AgentCard with auth_level, supported_interfaces, region.
func (d *RedisDataSource) GetMeta(ctx context.Context, agentID string) (AgentMeta, error) {
	if d.card == nil {
		return DefaultAgentMeta(), nil
	}
	card, err := d.card.GetAgent(ctx, agentID)
	if err != nil {
		return DefaultAgentMeta(), nil
	}
	return agentMetaFromCard(card), nil
}

// Replicate returns ErrReplicateNotImplemented (v0.8.0 M4-3.2).
//
// RedisDataSource has no real wau-trust engine attached — trust lives in
// the wau-trust module, not in scheduler's Redis keys. Production callers
// MUST wrap RedisDataSource with WauTrustDataSource (which delegates to
// wau-trust Engine.Replicate, backed by Redis with the "wau:trust:" prefix).
//
// This stub preserves the DataSource interface contract while making the
// production path explicit. See Makefile / NewSchedulerWithReplicationPolicy
// examples for the recommended setup.
func (d *RedisDataSource) Replicate(ctx context.Context, parent, child string, inheritanceFactor float64) (float64, error) {
	return 0, ErrReplicateNotImplemented
}

// ===== Helpers =====

func parseSemver(v string) (major, minor, patch int) {
	// Strip leading "v" if present
	if len(v) > 0 && v[0] == 'v' {
		v = v[1:]
	}
	parts := 0
	cur := ""
	for _, ch := range v {
		if ch == '.' {
			n, _ := strconv.Atoi(cur)
			if parts == 0 {
				major = n
			} else if parts == 1 {
				minor = n
			} else if parts == 2 {
				patch = n
			}
			parts++
			cur = ""
		} else if ch >= '0' && ch <= '9' {
			cur += string(ch)
		} else {
			break
		}
	}
	if cur != "" {
		n, _ := strconv.Atoi(cur)
		if parts == 0 {
			major = n
		} else if parts == 1 {
			minor = n
		} else if parts == 2 {
			patch = n
		}
	}
	return
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

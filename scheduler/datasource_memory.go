package scheduler

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/wau/registry/registry"
)

// MemoryDataSource is an in-process DataSource implementation.
//
// Useful for:
//   - Unit tests (no Redis needed)
//   - Single-node dev environments
//   - CI without Redis
//
// NOT safe for production — use RedisDataSource.
type MemoryDataSource struct {
	mu sync.RWMutex

	// Time-series data per agent
	latencies   map[string][]float64
	successes   map[string]int
	totalTasks  map[string]int
	errorCount  map[string]int
	bandwidths  map[string]float64
	trustScores map[string]float64

	// Static data per agent
	metas map[string]AgentMeta

	// Kernel config
	config DataSourceConfig

	// Optional registry for availability/version meta
	card registry.Registry
}

// NewMemoryDataSource creates an in-memory DataSource.
func NewMemoryDataSource(card registry.Registry, opts ...DataSourceOption) *MemoryDataSource {
	cfg := DataSourceConfig{
		PreferredProtocols: []string{"A2A", "AFP"},
		KernelVersion:      "v0.5.1",
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &MemoryDataSource{
		latencies:   make(map[string][]float64),
		successes:   make(map[string]int),
		totalTasks:  make(map[string]int),
		errorCount:  make(map[string]int),
		bandwidths:  make(map[string]float64),
		trustScores: make(map[string]float64),
		metas:       make(map[string]AgentMeta),
		config:      cfg,
		card:        card,
	}
}

// Helper setters (test API)

// SetLatencies sets a list of recent latencies for p99 calculation.
func (d *MemoryDataSource) SetLatencies(agentID string, latencies []float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.latencies[agentID] = append([]float64(nil), latencies...)
}

// SetSuccess sets the success/total counts.
func (d *MemoryDataSource) SetSuccess(agentID string, success, total int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.successes[agentID] = success
	d.totalTasks[agentID] = total
}

// SetErrorCount sets the error count.
func (d *MemoryDataSource) SetErrorCount(agentID string, errors int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.errorCount[agentID] = errors
}

// SetBandwidth sets the available bandwidth in Mbps.
func (d *MemoryDataSource) SetBandwidth(agentID string, mbps float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bandwidths[agentID] = mbps
}

// SetTotalTasks sets the cumulative task count.
func (d *MemoryDataSource) SetTotalTasks(agentID string, total int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.totalTasks[agentID] = total
}

// SetTrustScore sets the trust score.
func (d *MemoryDataSource) SetTrustScore(agentID string, score float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.trustScores[agentID] = score
}

// SetMeta sets the agent metadata.
func (d *MemoryDataSource) SetMeta(agentID string, meta AgentMeta) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.metas[agentID] = meta
}

// ===== DataSource interface implementation =====

// LatencyScore returns p99 latency score.
func (d *MemoryDataSource) LatencyScore(ctx context.Context, agentID string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	lats, ok := d.latencies[agentID]
	if !ok || len(lats) == 0 {
		return 1.0, nil
	}
	p99 := percentile(lats, 0.99)
	if p99 <= 0.1 {
		return 1.0, nil
	}
	if p99 >= 5.0 {
		return 0.0, nil
	}
	score := 1.0 - math.Log10(p99/0.1)/math.Log10(5.0/0.1)
	if score < 0 {
		score = 0
	}
	return score, nil
}

// SuccessRate returns success rate.
func (d *MemoryDataSource) SuccessRate(ctx context.Context, agentID string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	success, ok1 := d.successes[agentID]
	total, ok2 := d.totalTasks[agentID]
	if !ok1 || !ok2 || total == 0 {
		return 0.5, nil
	}
	rate := float64(success) / float64(total)
	const prior = 0.7
	return prior*rate + (1-prior)*0.5, nil
}

// BandwidthScore returns bandwidth score.
func (d *MemoryDataSource) BandwidthScore(ctx context.Context, agentID string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, ok := d.bandwidths[agentID]
	if !ok {
		return 1.0, nil
	}
	if val >= 100 {
		return 1.0, nil
	}
	if val < 1 {
		return 0.1, nil
	}
	score := math.Log10(val) / 2.0
	if score > 1 {
		score = 1
	}
	if score < 0.1 {
		score = 0.1
	}
	return score, nil
}

// HistoryCount returns history score.
func (d *MemoryDataSource) HistoryCount(ctx context.Context, agentID string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, ok := d.totalTasks[agentID]
	if !ok {
		return 0.5, nil // no data → neutral
	}
	if val == 0 {
		return 0.3, nil // 0 tasks → floor
	}
	if val >= 1000 {
		return 1.0, nil
	}
	score := 0.3 + 0.7*(math.Log10(float64(val))/3.0)
	if score > 1 {
		score = 1
	}
	if score < 0.3 {
		score = 0.3
	}
	return score, nil
}

// ErrorRate returns error rate score.
func (d *MemoryDataSource) ErrorRate(ctx context.Context, agentID string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	errors, ok1 := d.errorCount[agentID]
	total, ok2 := d.totalTasks[agentID]
	if !ok1 || !ok2 || total == 0 {
		return 1.0, nil
	}
	rate := float64(errors) / float64(total)
	return 1.0 - rate, nil
}

// Availability returns uptime ratio.
func (d *MemoryDataSource) Availability(ctx context.Context, agentID string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.card == nil {
		return 1.0, nil
	}
	card, err := d.card.GetAgent(ctx, agentID)
	if err != nil {
		return 1.0, nil
	}
	if card.FirstSeen <= 0 {
		return 0.5, nil
	}
	now := time.Now().Unix()
	elapsed := now - card.FirstSeen
	if elapsed <= 0 {
		return 0.5, nil
	}
	if card.LastSeen <= card.FirstSeen {
		return 0.0, nil
	}
	uptime := float64(card.LastSeen-card.FirstSeen) / float64(elapsed)
	if uptime > 1.0 {
		uptime = 1.0
	}
	return uptime, nil
}

// VersionCompat returns semver compatibility.
func (d *MemoryDataSource) VersionCompat(ctx context.Context, agentID string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.card == nil {
		return 1.0, nil
	}
	card, err := d.card.GetAgent(ctx, agentID)
	if err != nil {
		return 1.0, nil
	}
	if card.Version == "" || d.config.KernelVersion == "" {
		return 1.0, nil
	}
	kernelMajor, kernelMinor, _ := parseSemver(d.config.KernelVersion)
	agentMajor, agentMinor, _ := parseSemver(card.Version)
	if kernelMajor != agentMajor {
		return 0.0, nil
	}
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
func (d *MemoryDataSource) GeoPenalty(ctx context.Context, agentID string, sourceRegion string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	meta := d.getMetaLocked(agentID)
	if sourceRegion == "" || meta.Region == "" {
		return 1.0, nil
	}
	if meta.Region == sourceRegion {
		return 1.0, nil
	}
	return 0.9, nil
}

// TrustScore returns the dynamic trust score.
func (d *MemoryDataSource) TrustScore(ctx context.Context, agentID string) (float64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, ok := d.trustScores[agentID]
	if !ok {
		return 0.5, nil
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
// Implementation: aligns with wau-trust's Engine.IsCold — only checks the
// trust key. Cold routing's primary goal is to give fresh agents a chance
// to accumulate trust data; once trust is recorded (even 1 RecordSuccess),
// the agent is no longer cold from a routing perspective.
//
// Why NOT also check tasks:total / metrics:
//   - Trust signal is the canonical "data exists" indicator for cold routing
//   - Metrics can lag behind trust (e.g. trust updated by external watchdog
//     before any L4 handler task runs) — we don't want to mis-classify such
//     agents as cold when wau-trust already considers them warm
//   - Keeping the semantics aligned with wau-trust.IsCold means callers
//     (cold routing policy in M4-1.3) get a single, consistent signal
func (d *MemoryDataSource) IsCold(ctx context.Context, agentID string) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, hasTrust := d.trustScores[agentID]
	return !hasTrust, nil
}

// GetMeta returns extended agent metadata.
func (d *MemoryDataSource) GetMeta(ctx context.Context, agentID string) (AgentMeta, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.getMetaLocked(agentID), nil
}

func (d *MemoryDataSource) getMetaLocked(agentID string) AgentMeta {
	meta, ok := d.metas[agentID]
	if !ok {
		if d.card != nil {
			card, err := d.card.GetAgent(context.Background(), agentID)
			if err == nil {
				return agentMetaFromCard(card)
			}
		}
		return DefaultAgentMeta()
	}
	return meta
}

// Helper: percentile (P99)
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

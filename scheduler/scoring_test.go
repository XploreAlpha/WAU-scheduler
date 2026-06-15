package scheduler

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// testLogger returns a no-op logger for tests.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newTestRedisDataSource creates a RedisDataSource for tests.
// Uses the same Redis instance as integration tests (or local).
// Each test uses a unique key prefix to avoid collisions.
func newTestRedisDataSource(t *testing.T, prefix string) *RedisDataSource {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	ds := NewRedisDataSource(client, nil, prefix)
	t.Cleanup(func() {
		// Clean up all test keys
		ctx := context.Background()
		keys, _ := client.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		_ = client.Close()
	})
	return ds
}

// =====================================================================
// 7-dim scoring test suite (W1: 11 维度变 15 维度真算的一部分)
// =====================================================================

func TestRedisDataSource_LatencyScore(t *testing.T) {
	ctx := context.Background()
	ds := newTestRedisDataSource(t, "wau:test:latency:")

	tests := []struct {
		name     string
		setValue float64
		set      bool
		want     float64
	}{
		{"no data → 1.0", 0, false, 1.0},
		{"p99=50ms → 1.0 (under threshold)", 0.05, true, 1.0},
		{"p99=100ms → 1.0 (at threshold)", 0.1, true, 1.0},
		{"p99=500ms → ~0.78 (log decay)", 0.5, true, 0.7781}, // log10(5)/log10(50) = 0.7781
		{"p99=1s → ~0.68", 1.0, true, 0.6826},
		{"p99=5s → 0.0 (at upper bound)", 5.0, true, 0.0},
		{"p99=10s → 0.0 (above upper bound)", 10.0, true, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				_ = ds.client.Set(ctx, ds.key("agent1", "latency:p99"), tt.setValue, 0).Err()
			}
			got, err := ds.LatencyScore(ctx, "agent1")
			if err != nil {
				t.Fatalf("LatencyScore: %v", err)
			}
			if absDelta(got, tt.want) > 0.01 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestRedisDataSource_SuccessRate(t *testing.T) {
	ctx := context.Background()
	ds := newTestRedisDataSource(t, "wau:test:success:")

	tests := []struct {
		name        string
		successes   int
		total       int
		setBoth     bool
		want        float64
		description string
	}{
		{"no data → 0.5", 0, 0, false, 0.5, "neutral"},
		{"100% success", 10, 10, true, 0.85, "0.7*1.0 + 0.3*0.5 = 0.85"},
		{"50% success", 5, 10, true, 0.5, "0.7*0.5 + 0.3*0.5 = 0.5"},
		{"0% success", 0, 10, true, 0.15, "0.7*0.0 + 0.3*0.5 = 0.15"},
		{"total=0 → 0.5", 0, 0, true, 0.5, "neutral"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setBoth {
				_ = ds.client.Set(ctx, ds.key("a", "success:last100"), tt.successes, 0).Err()
				_ = ds.client.Set(ctx, ds.key("a", "tasks:last100"), tt.total, 0).Err()
			}
			got, err := ds.SuccessRate(ctx, "a")
			if err != nil {
				t.Fatalf("SuccessRate: %v", err)
			}
			if absDelta(got, tt.want) > 0.01 {
				t.Errorf("%s: got %f, want %f (%s)", tt.name, got, tt.want, tt.description)
			}
		})
	}
}

func TestRedisDataSource_BandwidthScore(t *testing.T) {
	ctx := context.Background()
	ds := newTestRedisDataSource(t, "wau:test:bandwidth:")

	tests := []struct {
		name  string
		value float64
		set   bool
		want  float64
	}{
		{"no data → 1.0", 0, false, 1.0},
		{"100Mbps → 1.0", 100, true, 1.0},
		{"10Mbps → 0.5", 10, true, 0.5}, // log10(10)/2 = 0.5
		{"1Mbps → 0.0 (lower bound)", 1, true, 0.0},
		{"0.5Mbps → 0.1 (clamp)", 0.5, true, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				_ = ds.client.Set(ctx, ds.key("a", "bandwidth:mbps"), tt.value, 0).Err()
			}
			got, err := ds.BandwidthScore(ctx, "a")
			if err != nil {
				t.Fatalf("BandwidthScore: %v", err)
			}
			if absDelta(got, tt.want) > 0.05 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestRedisDataSource_HistoryCount(t *testing.T) {
	ctx := context.Background()
	ds := newTestRedisDataSource(t, "wau:test:history:")

	tests := []struct {
		name  string
		total int
		set   bool
		want  float64
	}{
		{"no data → 0.5", 0, false, 0.5},
		{"0 tasks → 0.3 (floor)", 0, true, 0.3},
		{"10 tasks → ~0.93 (3*log10(10)/3 + 0.3)", 10, true, 0.93},
		{"100 tasks → ~0.8 (log10(100)/3 + 0.3)", 100, true, 0.967},
		{"1000+ tasks → 1.0 (cap)", 1000, true, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				_ = ds.client.Set(ctx, ds.key("a", "tasks:total"), tt.total, 0).Err()
			}
			got, err := ds.HistoryCount(ctx, "a")
			if err != nil {
				t.Fatalf("HistoryCount: %v", err)
			}
			if absDelta(got, tt.want) > 0.05 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestRedisDataSource_ErrorRate(t *testing.T) {
	ctx := context.Background()
	ds := newTestRedisDataSource(t, "wau:test:error:")

	tests := []struct {
		name   string
		errors int
		total  int
		set    bool
		want   float64
	}{
		{"no data → 1.0", 0, 0, false, 1.0},
		{"0% errors → 1.0", 0, 10, true, 1.0},
		{"10% errors → 0.9", 1, 10, true, 0.9},
		{"50% errors → 0.5", 5, 10, true, 0.5},
		{"100% errors → 0.0", 10, 10, true, 0.0},
		{"total=0 → 1.0", 0, 0, true, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				_ = ds.client.Set(ctx, ds.key("a", "errors:last100"), tt.errors, 0).Err()
				_ = ds.client.Set(ctx, ds.key("a", "tasks:last100"), tt.total, 0).Err()
			}
			got, err := ds.ErrorRate(ctx, "a")
			if err != nil {
				t.Fatalf("ErrorRate: %v", err)
			}
			if absDelta(got, tt.want) > 0.01 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestRedisDataSource_TrustScore(t *testing.T) {
	ctx := context.Background()
	ds := newTestRedisDataSource(t, "wau:test:trust:")

	tests := []struct {
		name  string
		value float64
		set   bool
		want  float64
	}{
		{"no data → 0.5", 0, false, 0.5},
		{"trust 0.8", 0.8, true, 0.8},
		{"trust 1.0", 1.0, true, 1.0},
		{"trust 0.0", 0.0, true, 0.0},
		{"out-of-range high → clamp to 1.0", 1.5, true, 1.0},
		{"out-of-range low → clamp to 0.0", -0.5, true, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				_ = ds.client.Set(ctx, ds.key("a", "trust"), tt.value, 0).Err()
			}
			got, err := ds.TrustScore(ctx, "a")
			if err != nil {
				t.Fatalf("TrustScore: %v", err)
			}
			if absDelta(got, tt.want) > 0.01 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

// =====================================================================
// ScoringEngine 集成测试(向后兼容 + DataSource 路径)
// =====================================================================

func TestScoringEngine_BackwardCompat(t *testing.T) {
	// v0.6.0 调用方式:无 DataSource → 11 维用占位
	logger := testLogger()
	_ = logger

	// NewScoringEngine(reg, logger) 不接受 nil reg
	// 旧测试已经覆盖了 v0.6.0 的行为,这里只验证 11 维的 dimension 函数不 panic
	eng := &ScoringEngine{weights: DefaultWeights(), logger: logger}

	dims := DimensionScores{
		SkillMatch:      eng.calcSkillMatch([]string{"coding"}, []string{"coding"}),
		TrustScore:      eng.dimensionTrustScore(context.Background(), "fake"),
		HealthScore:     0.5,
		LatencyScore:    eng.dimensionLatencyScore(context.Background(), "fake"),
		LoadScore:       0.5,
		SuccessRate:     eng.dimensionSuccessRate(context.Background(), "fake"),
		NetworkPenalty:  1.0,
		BandwidthScore:  eng.dimensionBandwidthScore(context.Background(), "fake"),
		AuthLevel:       eng.dimensionAuthLevel(context.Background(), "fake"),
		ProtocolCompat:  eng.dimensionProtocolCompat(context.Background(), "fake"),
		HistoryCount:    eng.dimensionHistoryCount(context.Background(), "fake"),
		ErrorRate:       eng.dimensionErrorRate(context.Background(), "fake"),
		Availability:    eng.dimensionAvailability(context.Background(), "fake"),
		VersionCompat:   eng.dimensionVersionCompat(context.Background(), "fake"),
		GeoPenalty:      eng.dimensionGeoPenalty(context.Background(), "fake", "us"),
	}

	// Verify all 15 dims are within [0, 1]
	for name, v := range map[string]float64{
		"SkillMatch":      dims.SkillMatch,
		"TrustScore":      dims.TrustScore,
		"HealthScore":     dims.HealthScore,
		"LatencyScore":    dims.LatencyScore,
		"LoadScore":       dims.LoadScore,
		"SuccessRate":     dims.SuccessRate,
		"NetworkPenalty":  dims.NetworkPenalty,
		"BandwidthScore":  dims.BandwidthScore,
		"AuthLevel":       dims.AuthLevel,
		"ProtocolCompat":  dims.ProtocolCompat,
		"HistoryCount":    dims.HistoryCount,
		"ErrorRate":       dims.ErrorRate,
		"Availability":    dims.Availability,
		"VersionCompat":   dims.VersionCompat,
		"GeoPenalty":      dims.GeoPenalty,
	} {
		if v < 0 || v > 1 {
			t.Errorf("%s = %f, out of [0, 1]", name, v)
		}
	}

	// v0.6.0 backward compat: 11 dims should be v0.6.0 placeholder values
	if dims.TrustScore != 0.5 {
		t.Errorf("TrustScore backward compat: expected 0.5, got %f", dims.TrustScore)
	}
	if dims.LatencyScore != 1.0 {
		t.Errorf("LatencyScore backward compat: expected 1.0, got %f", dims.LatencyScore)
	}
	if dims.SuccessRate != 0.5 {
		t.Errorf("SuccessRate backward compat: expected 0.5, got %f", dims.SuccessRate)
	}
}

// =====================================================================
// Helpers
// =====================================================================

func absDelta(a, b float64) float64 {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d
}

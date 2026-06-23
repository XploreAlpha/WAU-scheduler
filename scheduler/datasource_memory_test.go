package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/wau/registry/registry"
)

// =====================================================================
// MemoryDataSource 单元测试 (W1: 7 维真算)
// No Redis required — runs in CI without external dependencies.
// =====================================================================

func TestMemoryDataSource_LatencyScore(t *testing.T) {
	ctx := context.Background()
	ds := NewMemoryDataSource(nil)

	tests := []struct {
		name      string
		latencies []float64
		want      float64
		desc      string
	}{
		{"no data → 1.0", nil, 1.0, "neutral default"},
		{"p99=50ms → 1.0 (under threshold)", []float64{0.05}, 1.0, "fast enough"},
		{"p99=500ms → 0.59", []float64{0.5}, 0.5884, "log decay"},
		{"p99=1s → 0.41", []float64{1.0}, 0.4116, "log decay"},
		{"p99=5s → 0.0 (upper bound)", []float64{5.0}, 0.0, "too slow"},
		{"p99=10s → 0.0", []float64{10.0}, 0.0, "very slow"},
		{"mixed → p99 of [0.05,0.1,0.5,1.0,5.0] = 1.0", []float64{0.05, 0.1, 0.5, 1.0, 5.0}, 0.4116, "p99 idx=3 of 5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds.SetLatencies("a", tt.latencies)
			got, err := ds.LatencyScore(ctx, "a")
			if err != nil {
				t.Fatalf("LatencyScore: %v", err)
			}
			if absDelta(got, tt.want) > 0.01 {
				t.Errorf("%s: got %f, want %f (%s)", tt.name, got, tt.want, tt.desc)
			}
		})
	}
}

func TestMemoryDataSource_SuccessRate(t *testing.T) {
	ctx := context.Background()
	ds := NewMemoryDataSource(nil)

	tests := []struct {
		name    string
		success int
		total   int
		want    float64
		desc    string
	}{
		{"no data → 0.5", 0, 0, 0.5, "neutral"},
		{"100% success", 10, 10, 0.85, "0.7*1.0 + 0.3*0.5"},
		{"50% success", 5, 10, 0.5, "0.7*0.5 + 0.3*0.5"},
		{"0% success", 0, 10, 0.15, "0.7*0.0 + 0.3*0.5"},
		{"total=0 → 0.5", 0, 0, 0.5, "no data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds.SetSuccess("a", tt.success, tt.total)
			got, err := ds.SuccessRate(ctx, "a")
			if err != nil {
				t.Fatalf("SuccessRate: %v", err)
			}
			if absDelta(got, tt.want) > 0.01 {
				t.Errorf("%s: got %f, want %f (%s)", tt.name, got, tt.want, tt.desc)
			}
		})
	}
}

func TestMemoryDataSource_BandwidthScore(t *testing.T) {
	ctx := context.Background()
	ds := NewMemoryDataSource(nil)

	tests := []struct {
		name string
		mbps float64
		want float64
	}{
		{"no data → 1.0", 0, 1.0},
		{"100Mbps → 1.0", 100, 1.0},
		{"10Mbps → 0.5", 10, 0.5},
		{"1Mbps → 0.1 (lower bound clamp)", 1, 0.1}, // log10(1) = 0, clamped to 0.1
		{"0.5Mbps → 0.1 (clamp)", 0.5, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mbps > 0 {
				ds.SetBandwidth("a", tt.mbps)
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

func TestMemoryDataSource_HistoryCount(t *testing.T) {
	ctx := context.Background()
	ds := NewMemoryDataSource(nil)

	tests := []struct {
		name  string
		total int
		want  float64
		set   bool
	}{
		{"no data → 0.5", 0, 0.5, false},
		{"0 tasks → 0.3 (floor)", 0, 0.3, true},  // explicitly set to 0
		{"10 tasks → ~0.53", 10, 0.5333, true},   // 0.3 + 0.7 * log10(10)/3
		{"100 tasks → ~0.77", 100, 0.7667, true}, // 0.3 + 0.7 * log10(100)/3
		{"1000 tasks → 1.0 (cap)", 1000, 1.0, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				ds.SetTotalTasks("a", tt.total)
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

func TestMemoryDataSource_ErrorRate(t *testing.T) {
	ctx := context.Background()
	ds := NewMemoryDataSource(nil)

	tests := []struct {
		name   string
		errors int
		total  int
		want   float64
	}{
		{"no data → 1.0", 0, 0, 1.0},
		{"0% errors → 1.0", 0, 10, 1.0},
		{"10% errors → 0.9", 1, 10, 0.9},
		{"50% errors → 0.5", 5, 10, 0.5},
		{"100% errors → 0.0", 10, 10, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.total > 0 {
				ds.SetErrorCount("a", tt.errors)
				ds.SetTotalTasks("a", tt.total)
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

func TestMemoryDataSource_TrustScore(t *testing.T) {
	ctx := context.Background()
	ds := NewMemoryDataSource(nil)

	tests := []struct {
		name  string
		score float64
		set   bool
		want  float64
	}{
		{"no data → 0.5", 0, false, 0.5},
		{"trust 0.8", 0.8, true, 0.8},
		{"trust 1.0", 1.0, true, 1.0},
		{"trust 0.0", 0.0, true, 0.0},
		{"out-of-range high → 1.0", 1.5, true, 1.0},
		{"out-of-range low → 0.0", -0.5, true, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				ds.SetTrustScore("a", tt.score)
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

// ============== IsCold 测试 (v0.8.0 M4-1) ==============
//
// IsCold 信号是冷路由(M4-1.3)的关键 — 必须严格区分 fresh agent vs warm agent。
// GetScore 永远对两者返 0.5,IsCold 是唯一信号。

func TestMemoryDataSource_IsCold(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		setupFunc func(*MemoryDataSource) // 每个 test 独立设数据,避免污染
		agentID   string
		wantCold  bool
		desc      string
	}{
		{
			name: "fresh agent → cold",
			setupFunc: func(d *MemoryDataSource) {
				// 不设任何数据
			},
			agentID:  "fresh-agent",
			wantCold: true,
			desc:     "no trust, no tasks → cold",
		},
		{
			name: "trust only → warm (no tasks yet)",
			setupFunc: func(d *MemoryDataSource) {
				d.SetTrustScore("t-only", 0.7)
			},
			agentID:  "t-only",
			wantCold: false,
			desc:     "trust set → warm even without task history (trust is canonical signal)",
		},
		{
			name: "tasks only (no trust) → cold",
			setupFunc: func(d *MemoryDataSource) {
				d.SetTotalTasks("k-only", 5)
			},
			agentID:  "k-only",
			wantCold: true,
			desc:     "tasks set but trust not recorded → cold (trust is canonical signal for IsCold)",
		},
		{
			name: "trust + 1 task → warm",
			setupFunc: func(d *MemoryDataSource) {
				d.SetTrustScore("warm-1", 0.6)
				d.SetTotalTasks("warm-1", 1)
			},
			agentID:  "warm-1",
			wantCold: false,
			desc:     "1 task done with trust → warm",
		},
		{
			name: "trust + 100 tasks → warm",
			setupFunc: func(d *MemoryDataSource) {
				d.SetTrustScore("warm-100", 0.3)
				d.SetTotalTasks("warm-100", 100)
			},
			agentID:  "warm-100",
			wantCold: false,
			desc:     "100 tasks done → warm regardless of trust value",
		},
		{
			name: "trust=0 (failure mode) + tasks → warm",
			setupFunc: func(d *MemoryDataSource) {
				d.SetTrustScore("failed", 0.0)
				d.SetTotalTasks("failed", 5)
			},
			agentID:  "failed",
			wantCold: false,
			desc:     "trust=0 means 'failed many times' → not cold (有数据)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 每个 subtest 用新 ds,避免 Map 污染
			ds := NewMemoryDataSource(nil)
			if tt.setupFunc != nil {
				tt.setupFunc(ds)
			}
			got, err := ds.IsCold(ctx, tt.agentID)
			if err != nil {
				t.Fatalf("IsCold: %v", err)
			}
			if got != tt.wantCold {
				t.Errorf("IsCold(%q) = %v, want %v (%s)", tt.agentID, got, tt.wantCold, tt.desc)
			}
		})
	}
}

// TestMemoryDataSource_IsCold_Concurrent: 并发设数据不会让 IsCold 误判
func TestMemoryDataSource_IsCold_Concurrent(t *testing.T) {
	ctx := context.Background()
	ds := NewMemoryDataSource(nil)

	// 并发设 trust + tasks
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			ds.SetTrustScore("hot", 0.5+float64(i%50)/100)
		}
		close(done)
	}()
	go func() {
		for i := 0; i < 100; i++ {
			ds.SetTotalTasks("hot", i+1)
		}
	}()
	<-done

	cold, err := ds.IsCold(ctx, "hot")
	if err != nil {
		t.Fatalf("IsCold: %v", err)
	}
	if cold {
		t.Error("after 100 concurrent SetTrust + SetTotalTasks, agent should be warm")
	}
}

func TestMemoryDataSource_GetMeta(t *testing.T) {
	ctx := context.Background()
	ds := NewMemoryDataSource(nil)

	// Default
	meta, err := ds.GetMeta(ctx, "unknown")
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if meta.AuthLevel != 1.0 {
		t.Errorf("default AuthLevel: got %f, want 1.0", meta.AuthLevel)
	}
	if len(meta.SupportedInterfaces) == 0 {
		t.Error("default SupportedInterfaces empty")
	}

	// Custom
	ds.SetMeta("Whis", AgentMeta{
		AuthLevel:           0.9,
		SupportedInterfaces: []string{"A2A", "AFP"},
		Region:              "us-west-1",
		KernelVersion:       "v0.7.0",
	})
	meta, _ = ds.GetMeta(ctx, "Whis")
	if meta.AuthLevel != 0.9 {
		t.Errorf("Whis AuthLevel: got %f, want 0.9", meta.AuthLevel)
	}
	if meta.Region != "us-west-1" {
		t.Errorf("Whis Region: got %s, want us-west-1", meta.Region)
	}
}

func TestMemoryDataSource_Availability(t *testing.T) {
	ctx := context.Background()

	// Without card → 1.0
	ds := NewMemoryDataSource(nil)
	got, _ := ds.Availability(ctx, "any")
	if got != 1.0 {
		t.Errorf("no card → got %f, want 1.0", got)
	}

	// With card
	now := time.Now().Unix()
	ds2 := NewMemoryDataSource(&mockRegistry{
		getAgent: func(ctx context.Context, id string) (*registry.AgentCard, error) {
			return &registry.AgentCard{
				ID:        id,
				FirstSeen: now - 100,
				LastSeen:  now - 10, // 90% uptime
			}, nil
		},
	})
	got, _ = ds2.Availability(ctx, "a")
	if absDelta(got, 0.9) > 0.01 {
		t.Errorf("uptime 90%%: got %f, want 0.9", got)
	}
}

func TestMemoryDataSource_VersionCompat(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		kernelVersion string
		agentVersion  string
		want          float64
	}{
		{"same version → 1.0", "v0.5.1", "v0.5.1", 1.0},
		{"same major, +1 minor → 0.8", "v0.5.1", "v0.6.0", 0.8},
		{"same major, -1 minor → 0.8", "v0.6.0", "v0.5.1", 0.8},
		{"same major, +3 minor → 0.3", "v0.5.1", "v0.8.0", 0.3},
		{"major mismatch → 0.0", "v0.5.1", "v1.0.0", 0.0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ds := NewMemoryDataSource(&mockRegistry{
				getAgent: func(ctx context.Context, id string) (*registry.AgentCard, error) {
					return &registry.AgentCard{ID: id, Version: tt.agentVersion}, nil
				},
			}, WithKernelVersion(tt.kernelVersion))
			got, _ := ds.VersionCompat(ctx, "a")
			if absDelta(got, tt.want) > 0.01 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestMemoryDataSource_GeoPenalty(t *testing.T) {
	ctx := context.Background()

	ds := NewMemoryDataSource(nil)
	ds.SetMeta("a", AgentMeta{Region: "us-west-1"})

	tests := []struct {
		name         string
		sourceRegion string
		want         float64
	}{
		{"no source → 1.0", "", 1.0},
		{"same region → 1.0", "us-west-1", 1.0},
		{"different region → 0.9", "eu-central-1", 0.9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := ds.GeoPenalty(ctx, "a", tt.sourceRegion)
			if absDelta(got, tt.want) > 0.01 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

// =====================================================================
// Helpers
// =====================================================================

// mockRegistry implements registry.Registry for tests.
type mockRegistry struct {
	getAgent func(ctx context.Context, id string) (*registry.AgentCard, error)
}

func (m *mockRegistry) Heartbeat(ctx context.Context, req *registry.HeartbeatRequest) error {
	return nil
}
func (m *mockRegistry) GetAgent(ctx context.Context, id string) (*registry.AgentCard, error) {
	return m.getAgent(ctx, id)
}
func (m *mockRegistry) GetAgents(ctx context.Context) ([]*registry.AgentCard, error) {
	return nil, nil
}
func (m *mockRegistry) GetOnlineAgents(ctx context.Context) ([]*registry.AgentCard, error) {
	return nil, nil
}
func (m *mockRegistry) GetLoad(ctx context.Context, id string) (*registry.AgentLoad, error) {
	return nil, nil
}
func (m *mockRegistry) GetStatus(ctx context.Context, id string) (*registry.AgentStatus, error) {
	return nil, nil
}
func (m *mockRegistry) Deregister(ctx context.Context, id string) error {
	return nil
}
func (m *mockRegistry) Close() error {
	return nil
}

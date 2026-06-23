package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/XploreAlpha/wau-trust/engine"
)

// testLoggerCold returns a no-op logger for cold routing tests.
func testLoggerCold() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// =====================================================================
// DefaultColdRoutingPolicy 测试
// =====================================================================

func TestDefaultColdRoutingPolicy(t *testing.T) {
	p := DefaultColdRoutingPolicy(testLoggerCold())
	if p == nil {
		t.Fatal("DefaultColdRoutingPolicy returned nil")
	}
	if p.ExploreBudget != DefaultExploreBudget {
		t.Errorf("ExploreBudget = %f, want %f", p.ExploreBudget, DefaultExploreBudget)
	}
	if p.WarmupThreshold != DefaultWarmupThreshold {
		t.Errorf("WarmupThreshold = %d, want %d", p.WarmupThreshold, DefaultWarmupThreshold)
	}
	if p.SkillFloor != DefaultSkillFloor {
		t.Errorf("SkillFloor = %f, want %f", p.SkillFloor, DefaultSkillFloor)
	}
	if p.Logger == nil {
		t.Error("Logger should be set even when caller passes nil")
	}
	if p.hashSeed == 0 {
		t.Error("hashSeed should be initialized (non-zero)")
	}
}

// =====================================================================
// normalize 测试 — 零值/无效值 → defaults
// =====================================================================

func TestColdRoutingPolicy_Normalize(t *testing.T) {
	tests := []struct {
		name             string
		input            *ColdRoutingPolicy
		wantBudget       float64
		wantThreshold    int
		wantFloor        float64
	}{
		{
			name:          "all defaults",
			input:         &ColdRoutingPolicy{},
			wantBudget:    DefaultExploreBudget,
			wantThreshold: DefaultWarmupThreshold,
			wantFloor:     DefaultSkillFloor,
		},
		{
			name:          "zero budget → default",
			input:         &ColdRoutingPolicy{ExploreBudget: 0, WarmupThreshold: 5, SkillFloor: 0.7},
			wantBudget:    DefaultExploreBudget,
			wantThreshold: 5,
			wantFloor:     0.7,
		},
		{
			name:          "negative budget → default",
			input:         &ColdRoutingPolicy{ExploreBudget: -0.1},
			wantBudget:    DefaultExploreBudget,
			wantThreshold: DefaultWarmupThreshold,
			wantFloor:     DefaultSkillFloor,
		},
		{
			name:          "budget > 1.0 → default",
			input:         &ColdRoutingPolicy{ExploreBudget: 1.5},
			wantBudget:    DefaultExploreBudget,
			wantThreshold: DefaultWarmupThreshold,
			wantFloor:     DefaultSkillFloor,
		},
		{
			name:          "budget == 1.0 → preserved (100% explore allowed)",
			input:         &ColdRoutingPolicy{ExploreBudget: 1.0},
			wantBudget:    1.0,
			wantThreshold: DefaultWarmupThreshold,
			wantFloor:     DefaultSkillFloor,
		},
		{
			name:          "threshold zero → default",
			input:         &ColdRoutingPolicy{WarmupThreshold: 0},
			wantBudget:    DefaultExploreBudget,
			wantThreshold: DefaultWarmupThreshold,
			wantFloor:     DefaultSkillFloor,
		},
		{
			name:          "skill floor > 1 → default",
			input:         &ColdRoutingPolicy{SkillFloor: 1.5},
			wantBudget:    DefaultExploreBudget,
			wantThreshold: DefaultWarmupThreshold,
			wantFloor:     DefaultSkillFloor,
		},
		{
			name:          "valid custom values → preserved",
			input:         &ColdRoutingPolicy{ExploreBudget: 0.25, WarmupThreshold: 20, SkillFloor: 0.8},
			wantBudget:    0.25,
			wantThreshold: 20,
			wantFloor:     0.8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.input.normalize()
			if tt.input.ExploreBudget != tt.wantBudget {
				t.Errorf("ExploreBudget = %f, want %f", tt.input.ExploreBudget, tt.wantBudget)
			}
			if tt.input.WarmupThreshold != tt.wantThreshold {
				t.Errorf("WarmupThreshold = %d, want %d", tt.input.WarmupThreshold, tt.wantThreshold)
			}
			if tt.input.SkillFloor != tt.wantFloor {
				t.Errorf("SkillFloor = %f, want %f", tt.input.SkillFloor, tt.wantFloor)
			}
			if tt.input.hashSeed == 0 {
				t.Error("hashSeed should be set after normalize")
			}
			if tt.input.Logger == nil {
				t.Error("Logger should be set after normalize")
			}
		})
	}
}

// =====================================================================
// ShouldExplore 测试 — 决定 explore 还是 warm
// =====================================================================

// diverseRequestID generates a unique request ID for distribution tests.
// Uses a counter-based scheme that produces 10,000+ distinct IDs.
func diverseRequestID(i int) string {
	return fmt.Sprintf("req-%05d-%s", i, string(rune('a'+i%26)))
}

func TestColdRoutingPolicy_ShouldExplore_Distribution(t *testing.T) {
	// Budget 0.5 → ~50% explore across many distinct request IDs
	p := NewColdRoutingPolicy(0.5, 10, 0.5, testLoggerCold())

	const N = 10000
	var exploreCount int
	for i := 0; i < N; i++ {
		if p.ShouldExplore(diverseRequestID(i)) {
			exploreCount++
		}
	}
	rate := float64(exploreCount) / float64(N)
	// Expect 0.5 ± 5% (allowing for hash distribution variance)
	if rate < 0.45 || rate > 0.55 {
		t.Errorf("explore rate = %f, expected ~0.5 (range 0.45-0.55)", rate)
	}
}

func TestColdRoutingPolicy_ShouldExplore_Deterministic(t *testing.T) {
	// Same requestID → same decision across multiple calls
	p := NewColdRoutingPolicy(0.5, 10, 0.5, testLoggerCold())

	for _, reqID := range []string{"req-1", "req-2", "req-foo", "req-bar"} {
		first := p.ShouldExplore(reqID)
		for i := 0; i < 100; i++ {
			if p.ShouldExplore(reqID) != first {
				t.Errorf("ShouldExplore(%q) not deterministic: first=%v, later call returned different value", reqID, first)
				break
			}
		}
	}
}

func TestColdRoutingPolicy_ShouldExplore_BudgetLow(t *testing.T) {
	// Very low budget → very low explore rate
	p := NewColdRoutingPolicy(0.01, 10, 0.5, testLoggerCold())

	const N = 1000
	var exploreCount int
	for i := 0; i < N; i++ {
		if p.ShouldExplore(diverseRequestID(i)) {
			exploreCount++
		}
	}
	rate := float64(exploreCount) / float64(N)
	if rate > 0.05 {
		t.Errorf("with budget=0.01, expected rate < 0.05, got %f", rate)
	}
}

func TestColdRoutingPolicy_ShouldExplore_NoRequestID(t *testing.T) {
	// Should still work without requestID (uses LCG fallback)
	p := NewColdRoutingPolicy(0.5, 10, 0.5, testLoggerCold())

	const N = 1000
	var exploreCount int
	for i := 0; i < N; i++ {
		if p.ShouldExplore("") {
			exploreCount++
		}
	}
	rate := float64(exploreCount) / float64(N)
	if rate < 0.40 || rate > 0.60 {
		t.Errorf("with budget=0.5 and no requestID, expected rate ~0.5, got %f", rate)
	}
}

// =====================================================================
// SelectCold 测试 — 从 cold pool 选最佳 agent
//
// 用 WauTrustDataSource 包装 ds + trust engine,这样 IsCold 走 trust
// engine(真实场景),而不是 MemoryDataSource 自己的 trustScores map。
// =====================================================================

// newColdRoutingTestRig creates a (trust engine, WauTrustDataSource, ScoringEngine)
// test rig where IsCold correctly tracks wau-trust state.
func newColdRoutingTestRig(t *testing.T) (engine.Engine, *ScoringEngine) {
	t.Helper()
	logger := testLoggerCold()
	trust := engine.NewMemoryEngine()
	ds := NewMemoryDataSource(nil)
	wrapped := NewWauTrustDataSource(ds, trust)
	eng := NewScoringEngineWithDataSource(nil, wrapped, logger)
	return trust, eng
}

func TestColdRoutingPolicy_SelectCold(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerCold()

	tests := []struct {
		name       string
		candidates []AgentScore
		setupFunc  func(engine.Engine)
		requestID  string
		wantAgent  string // expected agent; "" = nil (no match); "ANY" = any cold agent meeting criteria
	}{
		{
			name: "no cold agents → nil",
			candidates: []AgentScore{
				{AgentID: "warm-1", TotalScore: 0.9, Dimensions: DimensionScores{SkillMatch: 1.0}},
				{AgentID: "warm-2", TotalScore: 0.7, Dimensions: DimensionScores{SkillMatch: 1.0}},
			},
			setupFunc: func(tr engine.Engine) {
				_ = tr.RecordSuccess(ctx, "warm-1", 0.5)
				_ = tr.RecordSuccess(ctx, "warm-2", 0.5)
			},
			requestID: "test-1",
			wantAgent: "",
		},
		{
			name: "one cold agent meets criteria → selected",
			candidates: []AgentScore{
				{AgentID: "cold-1", TotalScore: 0.6, Dimensions: DimensionScores{SkillMatch: 0.8}},
			},
			setupFunc:  func(tr engine.Engine) {}, // cold-1 not in trust → cold
			requestID:  "test-2",
			wantAgent:  "cold-1",
		},
		{
			name: "cold agent below skill floor → skipped",
			candidates: []AgentScore{
				{AgentID: "cold-low-skill", TotalScore: 0.9, Dimensions: DimensionScores{SkillMatch: 0.3}},
			},
			setupFunc: func(tr engine.Engine) {},
			requestID: "test-3",
			wantAgent: "",
		},
		{
			name: "multiple cold agents → pick one (deterministic per requestID)",
			candidates: []AgentScore{
				{AgentID: "cold-a", TotalScore: 0.4, Dimensions: DimensionScores{SkillMatch: 0.7}},
				{AgentID: "cold-b", TotalScore: 0.5, Dimensions: DimensionScores{SkillMatch: 0.9}},
				{AgentID: "cold-c", TotalScore: 0.6, Dimensions: DimensionScores{SkillMatch: 0.6}},
			},
			setupFunc: func(tr engine.Engine) {},
			requestID: "test-multi-1", // specific requestID will deterministically pick one
			wantAgent: "ANY",         // any cold agent OK, just must not be nil
		},
		{
			name: "mixed warm + cold → only cold considered",
			candidates: []AgentScore{
				{AgentID: "warm-1", TotalScore: 0.95, Dimensions: DimensionScores{SkillMatch: 1.0}},
				{AgentID: "cold-1", TotalScore: 0.6, Dimensions: DimensionScores{SkillMatch: 0.8}},
				{AgentID: "warm-2", TotalScore: 0.7, Dimensions: DimensionScores{SkillMatch: 1.0}},
			},
			setupFunc: func(tr engine.Engine) {
				_ = tr.RecordSuccess(ctx, "warm-1", 0.5)
				_ = tr.RecordSuccess(ctx, "warm-2", 0.5)
			},
			requestID: "test-mixed-1",
			wantAgent: "cold-1", // only cold eligible
		},
		{
			name: "cold but skill floor failed → nil",
			candidates: []AgentScore{
				{AgentID: "cold-low-1", TotalScore: 0.5, Dimensions: DimensionScores{SkillMatch: 0.4}},
				{AgentID: "cold-low-2", TotalScore: 0.5, Dimensions: DimensionScores{SkillMatch: 0.2}},
			},
			setupFunc: func(tr engine.Engine) {},
			requestID: "test-all-low",
			wantAgent: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trust, eng := newColdRoutingTestRig(t)
			tt.setupFunc(trust)
			_ = trust // vet: trust is used via setupFunc but vet needs explicit read

			p := NewColdRoutingPolicy(0.10, 10, 0.5, logger)
			chosen := p.SelectCold(ctx, tt.candidates, eng, tt.requestID)

			if tt.wantAgent == "" {
				if chosen != nil {
					t.Errorf("expected nil, got agent %q", chosen.AgentID)
				}
				return
			}
			if tt.wantAgent == "ANY" {
				if chosen == nil {
					t.Fatal("expected any cold agent, got nil")
				}
				// Verify it's actually one of the candidates
				found := false
				for _, c := range tt.candidates {
					if c.AgentID == chosen.AgentID {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("chosen agent %q not in candidates", chosen.AgentID)
				}
				return
			}
			if chosen == nil {
				t.Fatalf("expected agent %q, got nil", tt.wantAgent)
			}
			if chosen.AgentID != tt.wantAgent {
				t.Errorf("expected agent %q, got %q", tt.wantAgent, chosen.AgentID)
			}
		})
	}
}

// TestColdRoutingPolicy_SelectCold_Deterministic: 同一 requestID 多次调用
// 返回同一 agent(consistency across retries)
func TestColdRoutingPolicy_SelectCold_Deterministic(t *testing.T) {
	ctx := context.Background()
	_, eng := newColdRoutingTestRig(t)
	logger := testLoggerCold()

	candidates := []AgentScore{
		{AgentID: "cold-a", TotalScore: 0.4, Dimensions: DimensionScores{SkillMatch: 0.7}},
		{AgentID: "cold-b", TotalScore: 0.5, Dimensions: DimensionScores{SkillMatch: 0.9}},
		{AgentID: "cold-c", TotalScore: 0.6, Dimensions: DimensionScores{SkillMatch: 0.6}},
	}

	p := NewColdRoutingPolicy(0.10, 10, 0.5, logger)
	const reqID = "consistent-retry-test"
	first := p.SelectCold(ctx, candidates, eng, reqID)
	for i := 0; i < 50; i++ {
		got := p.SelectCold(ctx, candidates, eng, reqID)
		if got == nil || got.AgentID != first.AgentID {
			t.Errorf("SelectCold(%q) inconsistent: first=%v, later=%v", reqID, first, got)
			break
		}
	}
}

// =====================================================================
// 端到端 cold routing 行为测试
// =====================================================================

// TestColdRoutingPolicy_EndToEnd: 验证完整流程
// - 多个 agent, 部分 cold 部分 warm
// - 跑 N 个不同 request IDs, 验证 cold agents 都被选到(分散)
func TestColdRoutingPolicy_EndToEnd(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerCold()

	trust := engine.NewMemoryEngine()
	ds := NewMemoryDataSource(nil)
	wrapped := NewWauTrustDataSource(ds, trust)
	eng := NewScoringEngineWithDataSource(nil, wrapped, logger)

	// Setup: 3 warm agents + 3 cold agents
	for _, a := range []string{"warm-a", "warm-b", "warm-c"} {
		_ = trust.RecordSuccess(ctx, a, 0.5)
	}
	// cold-a, cold-b, cold-c: not in trust = cold

	p := NewColdRoutingPolicy(0.50, 10, 0.5, logger) // 50% explore to maximize coverage

	candidates := []AgentScore{
		{AgentID: "warm-a", TotalScore: 0.9, Dimensions: DimensionScores{SkillMatch: 1.0}},
		{AgentID: "warm-b", TotalScore: 0.8, Dimensions: DimensionScores{SkillMatch: 0.9}},
		{AgentID: "warm-c", TotalScore: 0.7, Dimensions: DimensionScores{SkillMatch: 0.8}},
		{AgentID: "cold-a", TotalScore: 0.5, Dimensions: DimensionScores{SkillMatch: 0.9}},
		{AgentID: "cold-b", TotalScore: 0.4, Dimensions: DimensionScores{SkillMatch: 0.8}},
		{AgentID: "cold-c", TotalScore: 0.3, Dimensions: DimensionScores{SkillMatch: 0.7}},
	}

	const N = 200
	coldCounts := map[string]int{}

	for i := 0; i < N; i++ {
		reqID := diverseRequestID(i)
		if !p.ShouldExplore(reqID) {
			continue
		}
		chosen := p.SelectCold(ctx, candidates, eng, reqID)
		if chosen != nil && isColdAgent(chosen.AgentID) {
			coldCounts[chosen.AgentID]++
		}
	}

	// All 3 cold agents should get at least some selections (load balancing)
	for _, a := range []string{"cold-a", "cold-b", "cold-c"} {
		if coldCounts[a] == 0 {
			t.Errorf("cold agent %q never selected — explore not distributing (counts: %v)", a, coldCounts)
		}
	}
	t.Logf("cold distribution: %v (total=%d)", coldCounts, sum(coldCounts))
}

func sum(m map[string]int) int {
	total := 0
	for _, v := range m {
		total += v
	}
	return total
}

// isColdAgent is a test helper to identify cold agents by name prefix.
func isColdAgent(name string) bool {
	return len(name) >= 5 && name[:5] == "cold-"
}

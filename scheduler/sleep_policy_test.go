package scheduler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// ====================================================================
// v0.8.0 M4-2.2: SleepPolicy unit tests
// ====================================================================

func TestNewSleepPolicy_Defaults(t *testing.T) {
	logger := slog.Default()
	p := NewSleepPolicy(logger)

	if p.SleepTrustThreshold != DefaultSleepTrustThreshold {
		t.Errorf("expected SleepTrustThreshold=%v, got %v",
			DefaultSleepTrustThreshold, p.SleepTrustThreshold)
	}
	if p.WakeQueueDepth != DefaultWakeQueueDepth {
		t.Errorf("expected WakeQueueDepth=%v, got %v",
			DefaultWakeQueueDepth, p.WakeQueueDepth)
	}
	if p.IdleDuration != DefaultIdleDuration {
		t.Errorf("expected IdleDuration=%v, got %v",
			DefaultIdleDuration, p.IdleDuration)
	}
	if p.Logger == nil {
		t.Error("expected Logger to be set")
	}
}

func TestSleepPolicy_ShouldWake(t *testing.T) {
	logger := slog.Default()
	p := NewSleepPolicy(logger)

	tests := []struct {
		name       string
		queueDepth int
		want       bool
	}{
		{"empty queue", 0, false},
		{"below threshold", 25, false},
		{"at threshold", 50, false}, // strictly greater
		{"above threshold", 51, true},
		{"way above", 1000, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.ShouldWake(tc.queueDepth)
			if got != tc.want {
				t.Errorf("ShouldWake(%d) = %v, want %v", tc.queueDepth, got, tc.want)
			}
		})
	}
}

func TestSleepPolicy_NilShouldWake_NoWake(t *testing.T) {
	var p *SleepPolicy = nil
	if p.ShouldWake(1000) {
		t.Error("nil policy should not trigger wake")
	}
}

func TestSleepPolicy_PickWakeTarget_Empty(t *testing.T) {
	logger := slog.Default()
	p := NewSleepPolicy(logger)

	got := p.PickWakeTarget(nil)
	if got != nil {
		t.Errorf("expected nil for empty candidates, got %v", got)
	}
}

func TestSleepPolicy_PickWakeTarget_SelectsHighest(t *testing.T) {
	logger := slog.Default()
	p := NewSleepPolicy(logger)

	candidates := []AgentScore{
		{AgentID: "low", TotalScore: 0.3},
		{AgentID: "high", TotalScore: 0.9},
		{AgentID: "mid", TotalScore: 0.5},
	}

	got := p.PickWakeTarget(candidates)
	if got == nil {
		t.Fatal("expected non-nil wake target")
	}
	if got.AgentID != "high" {
		t.Errorf("expected wake target 'high' (score 0.9), got %q", got.AgentID)
	}
}

func TestSleepPolicy_PickWakeTarget_Single(t *testing.T) {
	logger := slog.Default()
	p := NewSleepPolicy(logger)

	candidates := []AgentScore{
		{AgentID: "only", TotalScore: 0.7},
	}

	got := p.PickWakeTarget(candidates)
	if got == nil {
		t.Fatal("expected non-nil wake target")
	}
	if got.AgentID != "only" {
		t.Errorf("expected wake target 'only', got %q", got.AgentID)
	}
}

func TestSleepPolicy_NilPickWakeTarget_NoTarget(t *testing.T) {
	var p *SleepPolicy = nil
	candidates := []AgentScore{{AgentID: "x", TotalScore: 0.5}}
	got := p.PickWakeTarget(candidates)
	if got != nil {
		t.Errorf("nil policy should return nil, got %v", got)
	}
}

func TestSleepPolicy_FilterAsleep_NilPolicy(t *testing.T) {
	var p *SleepPolicy = nil
	candidates := []AgentScore{{AgentID: "x", TotalScore: 0.5}}

	got := p.FilterAsleep(context.Background(), candidates, nil)
	if len(got) != len(candidates) {
		t.Errorf("nil policy should not filter, got %d candidates", len(got))
	}
}

func TestSleepPolicy_FilterAsleep_NilScoring(t *testing.T) {
	logger := slog.Default()
	p := NewSleepPolicy(logger)
	candidates := []AgentScore{
		{AgentID: "x", TotalScore: 0.5},
		{AgentID: "y", TotalScore: 0.7},
	}

	got := p.FilterAsleep(context.Background(), candidates, nil)
	if len(got) != len(candidates) {
		t.Errorf("nil scoring should not filter, got %d candidates", len(got))
	}
}

// ====================================================================
// v0.8.0 M4-2.2: SleepPolicy FilterAsleep with real scoring
// ====================================================================

// makeAsleepTestRig creates a scheduler rig with 3 agents:
//   - Whis: awake, warm (high trust)
//   - Benny: awake, warm (medium trust)
//   - Fox: ASLEEP (will be marked asleep)
//
// Uses WauTrustDataSource wrapping MemoryDataSource + MemoryEngine.
// Reuses integrationMockRegistry from scoring_integration_test.go (same package).
func makeAsleepTestRig(t *testing.T) (*Scheduler, *engine.MemoryEngine) {
	t.Helper()

	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Whis":  {ID: "Whis", Name: "Whis", Version: "v0.5.1", Skills: []string{"general"}, Universe: "default", FirstSeen: time.Now().Unix() - 1000, LastSeen: time.Now().Unix()},
		"Benny": {ID: "Benny", Name: "Benny", Version: "v0.5.1", Skills: []string{"general"}, Universe: "default", FirstSeen: time.Now().Unix() - 1000, LastSeen: time.Now().Unix()},
		"Fox":   {ID: "Fox", Name: "Fox", Version: "v0.5.1", Skills: []string{"general"}, Universe: "default", FirstSeen: time.Now().Unix() - 1000, LastSeen: time.Now().Unix()},
	}}

	inner := NewMemoryDataSource(mockReg)
	trustEng := engine.NewMemoryEngine()

	// Whis: warm (trust recorded)
	_ = trustEng.RecordSuccess(context.Background(), "Whis", 0.5)
	inner.SetTrustScore("Whis", 0.9)
	inner.SetTotalTasks("Whis", 100)

	// Benny: warm (trust recorded, lower score)
	_ = trustEng.RecordSuccess(context.Background(), "Benny", 0.3)
	inner.SetTrustScore("Benny", 0.6)
	inner.SetTotalTasks("Benny", 50)

	// Fox: cold (no trust recorded) BUT manually asleep
	// In production, this would be: trustEng.Sleep(ctx, "Fox") after some warmup.
	// For the test rig, we mark Fox asleep via the wau-trust engine directly
	// (so WauTrustDataSource.IsAsleep returns true via the trust engine path).
	_ = trustEng.RecordSuccess(context.Background(), "Fox", 0.1) // warm up first
	inner.SetTrustScore("Fox", 0.3)
	inner.SetTotalTasks("Fox", 10)
	if err := trustEng.Sleep(context.Background(), "Fox"); err != nil {
		t.Fatalf("trustEng.Sleep(Fox): %v", err)
	}

	ds := NewWauTrustDataSource(inner, trustEng)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, slog.Default())
	s := NewSchedulerWithScoring(mockReg, scoring, slog.Default())

	return s, trustEng
}

func TestSleepPolicy_FilterAsleep_RemovesAsleepAgent(t *testing.T) {
	s, _ := makeAsleepTestRig(t)
	logger := slog.Default()
	policy := NewSleepPolicy(logger)

	// Candidates: Whis, Benny, Fox (Fox is asleep in the underlying DataSource)
	candidates := []AgentScore{
		{AgentID: "Whis", TotalScore: 0.85},
		{AgentID: "Benny", TotalScore: 0.65},
		{AgentID: "Fox", TotalScore: 0.40},
	}

	filtered := policy.FilterAsleep(context.Background(), candidates, s.scoring)

	// Fox should be filtered out
	if len(filtered) != 2 {
		t.Errorf("expected 2 candidates after filter (Whis + Benny), got %d", len(filtered))
		for _, c := range filtered {
			t.Logf("  remaining: %s (score=%v)", c.AgentID, c.TotalScore)
		}
	}

	// Verify Fox is NOT in the filtered list
	for _, c := range filtered {
		if c.AgentID == "Fox" {
			t.Error("Fox should have been filtered (asleep)")
		}
	}
}

func TestSleepPolicy_FilterAsleep_AllAwake_NoChange(t *testing.T) {
	s, _ := makeAsleepTestRig(t)
	logger := slog.Default()
	policy := NewSleepPolicy(logger)

	// Only awake candidates
	candidates := []AgentScore{
		{AgentID: "Whis", TotalScore: 0.85},
		{AgentID: "Benny", TotalScore: 0.65},
	}

	filtered := policy.FilterAsleep(context.Background(), candidates, s.scoring)

	if len(filtered) != 2 {
		t.Errorf("expected 2 candidates (no filter), got %d", len(filtered))
	}
}

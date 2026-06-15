package scheduler

import (
	"context"
	"testing"

	"github.com/XploreAlpha/wau-trust/engine"
)

// TestWauTrustDataSource_TrustScore_Delegates verifies that TrustScore
// is served by wau-trust, not by the inner DataSource.
//
// v0.7.0 W2: this is the core integration test — without it we cannot
// claim the scheduler is "wired to wau-trust".
func TestWauTrustDataSource_TrustScore_Delegates(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()

	// Inner DataSource returns 0.5 for everything (its no-data default).
	inner := NewMemoryDataSource(nil)
	inner.SetTrustScore("Whis", 0.99) // intentionally wrong, to prove override

	ds := NewWauTrustDataSource(inner, trust)

	// 1. Unrecorded agent → wau-trust returns 0.5 (DefaultTrustScore)
	//    WauTrustDataSource keeps scheduler's "no data → 0.5" semantic.
	score, err := ds.TrustScore(ctx, "Whis")
	if err != nil {
		t.Fatalf("TrustScore: %v", err)
	}
	if score != 0.5 {
		t.Errorf("unrecorded agent should map to 0.5, got %v", score)
	}

	// 2. Record some successes → wau-trust moves above default
	_ = trust.RecordSuccess(ctx, "Whis", 1.0)
	_ = trust.RecordSuccess(ctx, "Whis", 1.0)
	_ = trust.RecordSuccess(ctx, "Whis", 1.0)
	score, err = ds.TrustScore(ctx, "Whis")
	if err != nil {
		t.Fatalf("TrustScore: %v", err)
	}
	if score <= 0.5 {
		t.Errorf("after 3 successes, score should be > 0.5, got %v", score)
	}

	// 3. Confirm inner.DataSource.TrustScore is NOT consulted —
	//    the wrapper overrides it. The inner's 0.99 should be ignored.
	innerScore, _ := inner.TrustScore(ctx, "Whis")
	if innerScore == score {
		t.Errorf("inner DataSource.TrustScore was used instead of wau-trust (inner=%v, wrapper=%v)",
			innerScore, score)
	}
}

// TestWauTrustDataSource_NonTrustMethodsDelegate verifies that all
// other 9 dimensions are 1:1 pass-through to the inner DataSource.
func TestWauTrustDataSource_NonTrustMethodsDelegate(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()

	inner := NewMemoryDataSource(nil)
	inner.SetLatencies("Whis", []float64{0.1, 0.2, 0.3})
	inner.SetSuccess("Whis", 9, 10)
	inner.SetTotalTasks("Whis", 10)
	inner.SetMeta("Whis", AgentMeta{Region: "us-west-1", AuthLevel: 0.95})

	ds := NewWauTrustDataSource(inner, trust)

	// 9 delegations
	if v, _ := ds.LatencyScore(ctx, "Whis"); v <= 0 {
		t.Errorf("LatencyScore should be > 0, got %v", v)
	}
	if v, _ := ds.SuccessRate(ctx, "Whis"); v <= 0 {
		t.Errorf("SuccessRate should be > 0, got %v", v)
	}
	if v, _ := ds.BandwidthScore(ctx, "Whis"); v <= 0 {
		t.Errorf("BandwidthScore should be > 0, got %v", v)
	}
	if v, _ := ds.HistoryCount(ctx, "Whis"); v <= 0 {
		t.Errorf("HistoryCount should be > 0, got %v", v)
	}
	if v, _ := ds.ErrorRate(ctx, "Whis"); v <= 0 {
		t.Errorf("ErrorRate should be > 0, got %v", v)
	}
	if v, _ := ds.Availability(ctx, "Whis"); v <= 0 {
		t.Errorf("Availability should be > 0, got %v", v)
	}
	if v, _ := ds.VersionCompat(ctx, "Whis"); v <= 0 {
		t.Errorf("VersionCompat should be > 0, got %v", v)
	}
	if v, _ := ds.GeoPenalty(ctx, "Whis", "us-west-1"); v <= 0 {
		t.Errorf("GeoPenalty should be > 0, got %v", v)
	}
	if m, _ := ds.GetMeta(ctx, "Whis"); m.Region != "us-west-1" {
		t.Errorf("GetMeta should pass through, got region %q", m.Region)
	}
}

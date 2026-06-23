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

// ============== IsCold 透传测试 (v0.8.0 M4-1) ==============

// TestWauTrustDataSource_IsCold_Delegates: IsCold 直接透传到 wau-trust engine,
// 不走 inner DataSource。这样 scheduler 冷路由信号源是 wau-trust(单一真相),
// 跟 MemoryEngine / RedisEngine.IsCold 语义一致。
func TestWauTrustDataSource_IsCold_Delegates(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()

	// inner DataSource 不应该被 IsCold 调到 — 设一个错误信号验证
	inner := NewMemoryDataSource(nil)
	inner.SetTrustScore("Whis", 0.99) // inner 认为 warm
	_ = inner.SetTotalTasks

	ds := NewWauTrustDataSource(inner, trust)

	// 1. fresh agent → wau-trust.IsCold=true(无 Record)
	cold, err := ds.IsCold(ctx, "fresh-agent")
	if err != nil {
		t.Fatalf("IsCold: %v", err)
	}
	if !cold {
		t.Error("fresh agent should be cold (no wau-trust history)")
	}

	// 2. Record 1 次 success → wau-trust.IsCold=false(warm)
	_ = trust.RecordSuccess(ctx, "Whis", 0.1)
	cold, err = ds.IsCold(ctx, "Whis")
	if err != nil {
		t.Fatalf("IsCold: %v", err)
	}
	if cold {
		t.Error("after RecordSuccess, agent should be warm regardless of inner DataSource state")
	}

	// 3. Reset 后 → wau-trust.IsCold=true(Reset = cold 重新 explore)
	_ = trust.Reset(ctx, "Whis")
	cold, err = ds.IsCold(ctx, "Whis")
	if err != nil {
		t.Fatalf("IsCold: %v", err)
	}
	if !cold {
		t.Error("after Reset, agent should be cold again")
	}
}

// TestWauTrustDataSource_IsCold_NotInnerSource: 显式验证 inner DataSource
// 不被 IsCold 调用。如果 inner 信号跟 wau-trust 冲突,IsCold 应该只听 wau-trust 的。
func TestWauTrustDataSource_IsCold_NotInnerSource(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()

	// inner DataSource 标 agent 为 warm(有 trust score)
	inner := NewMemoryDataSource(nil)
	inner.SetTrustScore("agent-1", 0.8)
	inner.SetTotalTasks("agent-1", 50)

	ds := NewWauTrustDataSource(inner, trust)

	// wau-trust 完全不知道 agent-1(fresh)
	// inner 认为 warm,IsCold 必须返 true(只听 wau-trust)
	cold, err := ds.IsCold(ctx, "agent-1")
	if err != nil {
		t.Fatalf("IsCold: %v", err)
	}
	if !cold {
		t.Error("IsCold should ignore inner DataSource and only listen to wau-trust")
	}
}

// TestWauTrustDataSource_Replicate_Delegates verifies that Replicate
// delegates to wau-trust's Engine.Replicate (v0.8.0 M4-3.2).
//
// Inner DataSource is intentionally given a "wrong" child score to prove
// that Replicate is served by wau-trust, not the inner DataSource.
//
// Expected behavior:
//   - parent "Whis" has trust ~0.85 in wau-trust (weight=0.7 → 0.5*0.3+0.7=0.85)
//   - Replicate("Whis", "Whis-Child", 0.8) returns engine.ReplicateTrust(0.85, 0.8, ...)
//     in [0.63, 0.73] (0.85*0.8 = 0.68 ± 0.05)
//   - inner DataSource is bypassed entirely
func TestWauTrustDataSource_Replicate_Delegates(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()

	// Inner DataSource has a wrong/old child score to prove override
	inner := NewMemoryDataSource(nil)
	inner.SetTrustScore("Whis-Child", 0.1) // intentionally wrong

	// Warm Whis in wau-trust (trust ~0.85 with weight=0.7)
	if err := trust.RecordSuccess(ctx, "Whis", 0.7); err != nil {
		t.Fatalf("RecordSuccess: %v", err)
	}

	ds := NewWauTrustDataSource(inner, trust)

	childScore, err := ds.Replicate(ctx, "Whis", "Whis-Child", engine.DefaultInheritanceFactor)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	// Whis trust = 0.85, factor = 0.8 → child in [0.63, 0.73]
	if childScore < 0.63 || childScore > 0.73 {
		t.Errorf("Replicate child score: expected ~0.68 ± 0.05 (range [0.63, 0.73]), got %v", childScore)
	}

	// Verify determinism: pure helper should produce same value
	expected := engine.ReplicateTrust(0.85, engine.DefaultInheritanceFactor, "Whis", "Whis-Child")
	if childScore != expected {
		t.Errorf("Replicate delegation: expected %v (via ReplicateTrust), got %v", expected, childScore)
	}

	// Verify child was actually written to wau-trust (proving delegation)
	stored, err := trust.GetScore(ctx, "Whis-Child")
	if err != nil {
		t.Fatalf("GetScore after Replicate: %v", err)
	}
	if stored != childScore {
		t.Errorf("stored child score: expected %v, got %v", childScore, stored)
	}
}

// TestWauTrustDataSource_Replicate_PropagatesErrors verifies that engine
// errors (ErrParentCold, ErrInvalidFactor) are returned unchanged through
// the WauTrustDataSource delegation.
func TestWauTrustDataSource_Replicate_PropagatesErrors(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()
	inner := NewMemoryDataSource(nil)
	ds := NewWauTrustDataSource(inner, trust)

	// Cold parent → engine.ErrParentCold
	_, err := ds.Replicate(ctx, "Cold-Parent", "Child", engine.DefaultInheritanceFactor)
	if err == nil || err.Error() == "" {
		t.Errorf("expected error for cold parent, got nil")
	}

	// Invalid factor → engine.ErrInvalidFactor
	if err := trust.RecordSuccess(ctx, "Whis", 0.5); err != nil {
		t.Fatalf("RecordSuccess: %v", err)
	}
	_, err = ds.Replicate(ctx, "Whis", "Child", -0.5)
	if err == nil {
		t.Errorf("expected error for invalid factor, got nil")
	}
}

package scheduler

import (
	"context"
	"testing"

	"github.com/XploreAlpha/wau-trust/engine"
)

// TestWauTrustDataSource_RecordSuccess_Delegates 验证 WauTrustDataSource.RecordSuccess
// 委托 wau-trust engine.RecordSuccess(v0.8.0 hotfix 4-prereq)
//
// 痛点(per [[project-m4-3-design-2026-06-23]] 决策 1):
//   kernel 不直接 import wau-trust,只能通过 wau-scheduler.WauTrustDataSource 调 trust 原语。
//   但 WauTrustDataSource 之前只暴露 Replicate / RollbackReplicate(都是 kernel ReplicateAgent 用的),
//   不暴露 RecordSuccess / RecordFailure。
//   WAU-core-kernel hotfix 4 集成测试需要先用 RecordSuccess 把 parent trust 拉到 0.8+
//   才能跑 happy path,否则会被 scheduler.ErrParentLowTrust 拒绝。
//
// 修法:在 WauTrustDataSource 上加 RecordSuccess / RecordFailure,
//   跟 TrustDataSource(同 module 的 facade)对齐(它已经有这 2 个方法)。
//   这样外部调用方(测试 / 生产 trust 注入路径)有统一 API。
//
// 这个测试验证:
//   1. 通过 WauTrustDataSource.RecordSuccess 写入,engine.IsCold 立刻翻成 false(warm)
//   2. 通过 WauTrustDataSource.RecordFailure 写入,trust 下降
//   3. 多次 RecordSuccess 累加,trust score 单调上升(EMA 公式)
//   4. inner DataSource 不被这些方法触达(只是委托)
func TestWauTrustDataSource_RecordSuccess_Delegates(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()

	// inner DataSource 故意返"warm + 高 trust"信号,证明 RecordSuccess 不走 inner
	inner := NewMemoryDataSource(nil)
	inner.SetTrustScore("Whis", 0.99) // inner 视角:已经 warm + 0.99
	inner.SetTotalTasks("Whis", 50)

	ds := NewWauTrustDataSource(inner, trust)

	// 1. fresh agent → wau-trust.IsCold=true(cold)
	cold, err := ds.IsCold(ctx, "Whis")
	if err != nil {
		t.Fatalf("IsCold fresh: %v", err)
	}
	if !cold {
		t.Fatal("fresh agent should be cold (no wau-trust history)")
	}

	// 2. 通过 WauTrustDataSource.RecordSuccess 写入 1 次
	if err := ds.RecordSuccess(ctx, "Whis", 0.5); err != nil {
		t.Fatalf("RecordSuccess: %v", err)
	}

	// 3. IsCold 应立刻翻成 false(warm)— 证明 RecordSuccess 真写到 wau-trust
	cold, err = ds.IsCold(ctx, "Whis")
	if err != nil {
		t.Fatalf("IsCold after record: %v", err)
	}
	if cold {
		t.Error("after WauTrustDataSource.RecordSuccess, agent should be warm (wau-trust signal)")
	}

	// 4. TrustScore 应该上升(具体值由 EMA 公式决定,这里只检查 > 默认 0.5)
	score, err := ds.TrustScore(ctx, "Whis")
	if err != nil {
		t.Fatalf("TrustScore: %v", err)
	}
	if score <= 0.5 {
		t.Errorf("after RecordSuccess, TrustScore should be > 0.5, got %v", score)
	}
}

// TestWauTrustDataSource_RecordSuccess_MultipleCalls 验证多次 RecordSuccess 累加
//
// 用途:hotfix 4 happy path 测试需要循环 RecordSuccess 把 parent trust 拉到 0.8+。
// 这个测试确保多次调用单调上升(EMA 公式)。
func TestWauTrustDataSource_RecordSuccess_MultipleCalls(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()
	ds := NewWauTrustDataSource(NewMemoryDataSource(nil), trust)

	prevScore := 0.5 // default
	for i := 0; i < 5; i++ {
		if err := ds.RecordSuccess(ctx, "Whis", 1.0); err != nil {
			t.Fatalf("RecordSuccess[%d]: %v", i, err)
		}
		score, err := ds.TrustScore(ctx, "Whis")
		if err != nil {
			t.Fatalf("TrustScore[%d]: %v", i, err)
		}
		if score < prevScore {
			t.Errorf("iter %d: score decreased (prev=%v, current=%v)", i, prevScore, score)
		}
		prevScore = score
		t.Logf("iter %d: score=%v", i, score)
	}

	// 5 次 weight=1.0 后 trust 应该远高于 0.5(EMA 朝 weight 收敛)
	if prevScore <= 0.5 {
		t.Errorf("after 5 RecordSuccess(weight=1.0), score should be >> 0.5, got %v", prevScore)
	}
}

// TestWauTrustDataSource_RecordFailure_Delegates 验证 RecordFailure 委托 engine
//
// 跟 RecordSuccess 对称,但效果相反(降低 trust)。
func TestWauTrustDataSource_RecordFailure_Delegates(t *testing.T) {
	ctx := context.Background()
	trust := engine.NewMemoryEngine()
	ds := NewWauTrustDataSource(NewMemoryDataSource(nil), trust)

	// 先 warm up
	if err := ds.RecordSuccess(ctx, "Whis", 1.0); err != nil {
		t.Fatalf("RecordSuccess warmup: %v", err)
	}
	scoreBefore, err := ds.TrustScore(ctx, "Whis")
	if err != nil {
		t.Fatalf("TrustScore before: %v", err)
	}
	if scoreBefore <= 0.5 {
		t.Fatalf("warmup failed, score=%v", scoreBefore)
	}

	// Record failure
	if err := ds.RecordFailure(ctx, "Whis", 1.0); err != nil {
		t.Fatalf("RecordFailure: %v", err)
	}
	scoreAfter, err := ds.TrustScore(ctx, "Whis")
	if err != nil {
		t.Fatalf("TrustScore after: %v", err)
	}

	// failure 至少不应该让 score 上升(EMA 朝 failure 收敛或 stay)
	if scoreAfter > scoreBefore {
		t.Errorf("after RecordFailure, score should not increase (before=%v, after=%v)",
			scoreBefore, scoreAfter)
	}
	t.Logf("failure: before=%v, after=%v", scoreBefore, scoreAfter)
}
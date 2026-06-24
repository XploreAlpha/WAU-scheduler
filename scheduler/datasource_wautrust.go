package scheduler

import (
	"context"

	"github.com/XploreAlpha/wau-trust/engine"
)

// WauTrustDataSource wraps another DataSource and overrides TrustScore
// (and IsCold, since v0.8.0 M4-1) to use wau-trust's Engine.
//
// v0.7.0 W2 integration: the scheduler no longer reads its own internal
// EMA-style trust key — it delegates to the dedicated wau-trust module,
// which is independently testable, reusable, and explainable.
//
// All other 9 dimensions (LatencyScore / SuccessRate / BandwidthScore /
// HistoryCount / ErrorRate / Availability / VersionCompat / GeoPenalty /
// GetMeta) are delegated to the inner DataSource unchanged.
type WauTrustDataSource struct {
	inner DataSource
	trust engine.Engine
}

// NewWauTrustDataSource returns a DataSource that delegates everything
// to `inner` except TrustScore (and IsCold), which are served by wau-trust's
// Engine.
//
// In production this is typically used as:
//
//	ds := scheduler.NewWauTrustDataSource(
//	    scheduler.NewRedisDataSource(redisClient, card, "wau:"),
//	    wautrust.NewRedisEngine(redisClient, "wau:trust:"),
//	)
func NewWauTrustDataSource(inner DataSource, trust engine.Engine) *WauTrustDataSource {
	return &WauTrustDataSource{inner: inner, trust: trust}
}

// NewDefaultWauTrustDataSource v0.8.0 M4-3.3: 简便工厂,用 in-process MemoryEngine
// 构造 *WauTrustDataSource(跟 NewDefaultTrustDataSource 行为一致但返 *WauTrustDataSource,
// 提供 Replicate 等 wau-trust 原语访问)。
//
// 为什么需要这个:WAU-core-kernel M4-3.3 ReplicateAgent 流程需要调 Replicate 原语,
// 而 *TrustDataSource(facade)不暴露 Replicate。kernel 不应该直接 import wau-trust
// (per [[feedback-dev-style]] + v0.7.0 W2 设计 'kernel 通过 wau-scheduler proxy trust'),
// 所以 wau-scheduler 加这个工厂方法把 *WauTrustDataSource + MemoryEngine 一并包好。
//
// Replicate 行为契约: 委托 wau-trust engine.Replicate,返回 (child_trust, error)。
// Errors 透传: engine.ErrParentCold, engine.ErrInvalidFactor。
//
// 用法:
//
//	trustDS := wauScheduler.NewDefaultWauTrustDataSource()  // *WauTrustDataSource
//	childScore, err := trustDS.Replicate(ctx, "parent", "child", 0.8)
func NewDefaultWauTrustDataSource() *WauTrustDataSource {
	return &WauTrustDataSource{
		inner: NewMemoryDataSource(nil),  // 占位 inner,无 registry 不影响 TrustScore/IsCold/IsAsleep/Replicate
		trust: engine.NewMemoryEngine(),
	}
}

// TrustScore delegates to wau-trust engine.
//
// wau-trust's Engine.GetScore returns (0.0, nil) when an agent has never
// been recorded, but to preserve the scheduler's "no data → 0.5" semantic
// we coerce 0.0 → 0.5. This keeps the 15-dim TotalScore stable for new
// agents and prevents an "untouched agent" from being treated as
// "fully distrusted" (which would be a sad default for a Network OS).
func (d *WauTrustDataSource) TrustScore(ctx context.Context, agentID string) (float64, error) {
	score, err := d.trust.GetScore(ctx, agentID)
	if err != nil {
		return 0.5, err
	}
	if score == engine.DefaultTrustScore {
		// Fresh agent: wau-trust returns 0.5 (its DefaultTrustScore).
		// Treat as "no data" → 0.5 (scheduler's neutral default).
		return 0.5, nil
	}
	return score, nil
}

// IsCold delegates to wau-trust's engine.IsCold (v0.8.0 M4-1).
//
// Unlike TrustScore which uses wau-trust as the trust source but defaults
// to 0.5 on missing data, IsCold uses the engine's "fresh vs warm" signal
// directly — it does NOT degrade to "false" (warm) on error, since that's
// the most harmful failure mode (would route fresh agents through the
// warm pool without exploration).
//
// On error: we return false (treat as warm) to avoid blocking routing —
// cold routing is an optimization, not a correctness requirement.
func (d *WauTrustDataSource) IsCold(ctx context.Context, agentID string) (bool, error) {
	cold, err := d.trust.IsCold(ctx, agentID)
	if err != nil {
		// Conservative: route through warm pool on error (avoid blocking)
		return false, err
	}
	return cold, nil
}

// IsAsleep delegates to wau-trust's engine.IsAsleep (v0.8.0 M4-2).
//
// wau-trust's Engine.IsAsleep is the canonical sleep state owner — agents
// are marked asleep via wau-trust Engine.Sleep and woken via Wake (or
// implicitly via Reset).
//
// On error: we return false (treat as awake) to avoid blocking routing —
// sleep is a resource-saving optimization, not a correctness requirement.
// An asleep agent falsely treated as awake will get scheduled normally;
// the worst case is we lose some efficiency, not correctness.
func (d *WauTrustDataSource) IsAsleep(ctx context.Context, agentID string) (bool, error) {
	asleep, err := d.trust.IsAsleep(ctx, agentID)
	if err != nil {
		// Conservative: treat as awake on error (avoid blocking routing)
		return false, err
	}
	return asleep, nil
}

// Replicate delegates to wau-trust's engine.Replicate (v0.8.0 M4-3.2).
//
// This is the production path for self-replication: wau-trust's Engine.Replicate
// computes the child trust via deterministic jitter (FNV-1a hash of
// "parent:child") and writes the result to the underlying store
// (MemoryEngine for tests, RedisEngine for production).
//
// Scheduler-level policy gates (MinParentTrust, MaxChildrenPerParent) are
// NOT enforced here — those live in ReplicationPolicy.CanReplicate, called
// before this method by Scheduler.Replicate.
//
// Errors propagate unchanged from engine:
//   - engine.ErrParentCold
//   - engine.ErrInvalidFactor
func (d *WauTrustDataSource) Replicate(ctx context.Context, parent, child string, inheritanceFactor float64) (float64, error) {
	return d.trust.Replicate(ctx, parent, child, inheritanceFactor)
}

// RollbackReplicate delegates to wau-trust's engine.RollbackReplicate
// (v0.8.0 hotfix 1).
//
// Use case: WAU-core-kernel.ReplicateAgent does 3 writes
// (trust → registry.Heartbeat → counter). If trust write succeeds but
// registry.Heartbeat fails, the kernel calls this to undo the trust write
// and avoid leaving an orphan trust entry.
//
// Trampling check: engine.RollbackReplicate verifies the child's most
// recent history entry is ReasonReplicate before deleting. If a concurrent
// writer has modified the child since the Replicate, returns
// engine.ErrNotReplicated and does NOT delete (caller should log warning).
//
// Idempotent: returns nil if the child has no history (never replicated,
// already rolled back, or Reset since).
//
// Errors propagate unchanged from engine:
//   - engine.ErrNotReplicated: trampling check failed
func (d *WauTrustDataSource) RollbackReplicate(ctx context.Context, parent, child string) error {
	return d.trust.RollbackReplicate(ctx, parent, child)
}

// ===== Delegated methods (1:1 pass-through) =====

func (d *WauTrustDataSource) LatencyScore(ctx context.Context, agentID string) (float64, error) {
	return d.inner.LatencyScore(ctx, agentID)
}

func (d *WauTrustDataSource) SuccessRate(ctx context.Context, agentID string) (float64, error) {
	return d.inner.SuccessRate(ctx, agentID)
}

func (d *WauTrustDataSource) BandwidthScore(ctx context.Context, agentID string) (float64, error) {
	return d.inner.BandwidthScore(ctx, agentID)
}

func (d *WauTrustDataSource) HistoryCount(ctx context.Context, agentID string) (float64, error) {
	return d.inner.HistoryCount(ctx, agentID)
}

func (d *WauTrustDataSource) ErrorRate(ctx context.Context, agentID string) (float64, error) {
	return d.inner.ErrorRate(ctx, agentID)
}

func (d *WauTrustDataSource) Availability(ctx context.Context, agentID string) (float64, error) {
	return d.inner.Availability(ctx, agentID)
}

func (d *WauTrustDataSource) VersionCompat(ctx context.Context, agentID string) (float64, error) {
	return d.inner.VersionCompat(ctx, agentID)
}

func (d *WauTrustDataSource) GeoPenalty(ctx context.Context, agentID string, sourceRegion string) (float64, error) {
	return d.inner.GeoPenalty(ctx, agentID, sourceRegion)
}

func (d *WauTrustDataSource) GetMeta(ctx context.Context, agentID string) (AgentMeta, error) {
	return d.inner.GetMeta(ctx, agentID)
}

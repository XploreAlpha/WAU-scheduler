package scheduler

import (
	"context"

	"github.com/XploreAlpha/wau-trust/engine"
)

// TrustDataSource v0.7.0 W2: kernel-friendly 入口,封装 wau-trust engine
//
// wau-core-kernel 通过 NewTrustDataSource 拿到这个 type,只调 TrustScore
// 和 TrustExplanation(都是直接代理 wau-trust engine),不直接 import
// wau-trust 也不关心底层是 MemoryEngine 还是 RedisEngine。
//
// 之所以不暴露 engine.Engine 给 kernel:
//   - kernel 不应该直接依赖 wau-trust 的 module path(go.sum / GOPROXY 在
//     内网环境拉不到 XploreAlpha/wau-trust 这条路径)
//   - scheduler 是 wau-trust 的"集成 owner",kernel 只跟 scheduler 打交道
//
// 行为契约:
//   - TrustScore: wau-trust 0.0 → 0.5 兼容(新 agent 不被当 0 分)
//   - TrustExplanation: wau-trust.Explain 完整透传
type TrustDataSource struct {
	trust engine.Engine
}

// NewTrustDataSource 用给定的 wau-trust engine 构造一个 TrustDataSource
//
// 典型用法:
//
//	ds := scheduler.NewTrustDataSource(wauTrust.NewMemoryEngine())
//
// 或生产环境(wau-trust 0.0.1+ 会有 RedisEngine):
//
//	ds := scheduler.NewTrustDataSource(wauTrust.NewRedisEngine(rdb, "wau:trust:"))
func NewTrustDataSource(trust engine.Engine) *TrustDataSource {
	return &TrustDataSource{trust: trust}
}

// NewDefaultTrustDataSource v0.7.0 W2 简便工厂:用 in-process MemoryEngine
// 构造一个 TrustDataSource,适合开发 / 本机测试 / 不需要 trust 持久化
// 的场景(kernel main.go 不直接 import wau-trust)。
//
// v0.7.1+ 会改用 NewRedisEngine 持久化。
func NewDefaultTrustDataSource() *TrustDataSource {
	return &TrustDataSource{trust: engine.NewMemoryEngine()}
}

// TrustScore 返回 agent 的 trust score,fresh agent 返回 0.5(默认)。
func (d *TrustDataSource) TrustScore(ctx context.Context, agentID string) (float64, error) {
	score, err := d.trust.GetScore(ctx, agentID)
	if err != nil {
		return 0.5, err
	}
	if score == engine.DefaultTrustScore {
		// wau-trust 内部已经把 0.5 当默认值返回,无需 coerce。
		// 这里显式保留以跟 WauTrustDataSource 行为一致。
		return engine.DefaultTrustScore, nil
	}
	return score, nil
}

// TrustExplanation 返回 trust score 的可解释信息(factors, history, reason)。
// 调用方负责把返回结构原样 JSON 序列化给 API 客户端。
func (d *TrustDataSource) TrustExplanation(ctx context.Context, agentID string) (engine.TrustExplanation, error) {
	return d.trust.Explain(ctx, agentID)
}

// RecordSuccess 透传 wau-trust RecordSuccess,便于测试 / 手动注入 trust 历史
func (d *TrustDataSource) RecordSuccess(ctx context.Context, agentID string, weight float64) error {
	return d.trust.RecordSuccess(ctx, agentID, weight)
}

// RecordFailure 透传 wau-trust RecordFailure
func (d *TrustDataSource) RecordFailure(ctx context.Context, agentID string, weight float64) error {
	return d.trust.RecordFailure(ctx, agentID, weight)
}

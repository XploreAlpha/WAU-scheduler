package scheduler

import (
	"context"
	"log/slog"
	"sync"

	"github.com/wau/registry/registry"
)

// ScoringEngine 评分引擎 (线程安全, 15 维度)
//
// v0.7.0 W1: 4 真 + 11 占位 → 4 真 + 7 真(本文件) + 4 占位(W2 真算)
// v0.7.0 W2: 15 维全部真算 + TrustScore 接 wau-trust
type ScoringEngine struct {
	reg     registry.Registry
	ds      DataSource // v0.7.0 W1 新增
	weights Weights
	logger  *slog.Logger
	mu      sync.RWMutex // 保护并发访问
}

// NewScoringEngine 创建评分引擎(向后兼容,v0.7.0 之前调用方式不变)
func NewScoringEngine(reg registry.Registry, logger *slog.Logger) *ScoringEngine {
	return &ScoringEngine{
		reg:     reg,
		weights: DefaultWeights(),
		logger:  logger,
		// ds: nil — 11 维度用占位(W1 之前的行为)
	}
}

// NewScoringEngineWithDataSource 创建评分引擎 with DataSource
// v0.7.0 W1 推荐使用此构造器
func NewScoringEngineWithDataSource(reg registry.Registry, ds DataSource, logger *slog.Logger) *ScoringEngine {
	return &ScoringEngine{
		reg:     reg,
		ds:      ds,
		weights: DefaultWeights(),
		logger:  logger,
	}
}

// ScoreRequest 评分请求
type ScoreRequest struct {
	RequiredSkills []string
	IntentType     string
	Urgency        string
	SourceUniverse string
	SourceRegion   string // v0.7.0 W2: GeoPenalty
}

// ScoreAgents 对Agent列表进行评分 (线程安全)
func (e *ScoringEngine) ScoreAgents(ctx context.Context, agentIDs []string, req *ScoreRequest) ([]AgentScore, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var scores []AgentScore

	for _, agentID := range agentIDs {
		score, err := e.scoreAgent(ctx, agentID, req)
		if err != nil {
			e.logger.Warn("Failed to score agent", "agent", agentID, "error", err)
			continue
		}
		scores = append(scores, score)
	}

	// 按分数排序
	for i := 0; i < len(scores); i++ {
		for j := i + 1; j < len(scores); j++ {
			if scores[j].TotalScore > scores[i].TotalScore {
				scores[i], scores[j] = scores[j], scores[i]
			}
		}
	}

	// 设置排名
	for i := range scores {
		scores[i].Rank = i + 1
	}

	return scores, nil
}

func (e *ScoringEngine) scoreAgent(ctx context.Context, agentID string, req *ScoreRequest) (AgentScore, error) {
	card, err := e.reg.GetAgent(ctx, agentID)
	if err != nil {
		return AgentScore{}, err
	}

	load, _ := e.reg.GetLoad(ctx, agentID)

	// 计算各维度分数
	dims := DimensionScores{
		// 1. Skill匹配度 (0-1) — 已有,W1 维持
		SkillMatch: e.calcSkillMatch(card.Skills, req.RequiredSkills),

		// 2. Trust分数 (0-1) — W1 末已接 wau-trust(WauTrustDataSource 包装器),
		// 失败时降级到 0.5(无数据 default),保持向后兼容
		// v0.7.0 W2 进一步通过 HTTP endpoint `GET /registry/agents/{name}/trust` 暴露
		TrustScore: e.dimensionTrustScore(ctx, agentID),

		// 3. 健康状态 (0-1) — 已有,W1 维持
		HealthScore: e.calcHealthScore(load),

		// 4. 延迟分数 (0-1) — v0.7.0 W1 真算(从 Redis p99)
		LatencyScore: e.dimensionLatencyScore(ctx, agentID),

		// 5. 负载分数 (0-1) — 已有,W1 维持
		LoadScore: e.calcLoadScore(load),

		// 6. 成功率 (0-1) — v0.7.0 W1 真算(从 last100)
		SuccessRate: e.dimensionSuccessRate(ctx, agentID),

		// 7. 网络惩罚 (0.7-1.0) — 已有,W1 维持
		NetworkPenalty: e.calcNetworkPenalty(card.Universe, req.SourceUniverse),

		// 8. 带宽可用性 (0-1) — v0.7.0 W1 真算(从心跳)
		BandwidthScore: e.dimensionBandwidthScore(ctx, agentID),

		// 9. 认证级别 (0-1) — v0.7.0 W1 真算(从 AgentMeta)
		AuthLevel: e.dimensionAuthLevel(ctx, agentID),

		// 10. 协议兼容性 (0-1) — v0.7.0 W1 真算(intersection of preferred ∩ supported)
		ProtocolCompat: e.dimensionProtocolCompat(ctx, agentID),

		// 11. 历史交互次数 (0-1) — v0.7.0 W1 真算(从 tasks:total)
		HistoryCount: e.dimensionHistoryCount(ctx, agentID),

		// 12. 错误率 (0-1) — v0.7.0 W1 真算(从 errors:last100)
		ErrorRate: e.dimensionErrorRate(ctx, agentID),

		// 13. 可用性 (0-1) — W1 末已真算:LastSeen/FirstSeen uptime ratio(Redis/Memory DS)
		Availability: e.dimensionAvailability(ctx, agentID),

		// 14. 版本兼容性 (0-1) — W1 末已真算:semver 同 major → 1.0,跨 major 衰减(Redis/Memory DS)
		VersionCompat: e.dimensionVersionCompat(ctx, agentID),

		// 15. 地理位置惩罚 (0.9-1.0) — W1 末已真算:同 region → 1.0,跨 region → 0.9(Redis/Memory DS)
		GeoPenalty: e.dimensionGeoPenalty(ctx, agentID, req.SourceRegion),
	}

	// 计算总分
	total := dims.SkillMatch*e.weights.SkillMatch +
		dims.TrustScore*e.weights.TrustScore +
		dims.HealthScore*e.weights.HealthScore +
		dims.LatencyScore*e.weights.LatencyScore +
		dims.LoadScore*e.weights.LoadScore +
		dims.SuccessRate*e.weights.SuccessRate +
		dims.NetworkPenalty*e.weights.NetworkPenalty +
		dims.BandwidthScore*e.weights.BandwidthScore +
		dims.AuthLevel*e.weights.AuthLevel +
		dims.ProtocolCompat*e.weights.ProtocolCompat +
		dims.HistoryCount*e.weights.HistoryCount +
		dims.ErrorRate*e.weights.ErrorRate +
		dims.Availability*e.weights.Availability +
		dims.VersionCompat*e.weights.VersionCompat +
		dims.GeoPenalty*e.weights.GeoPenalty

	return AgentScore{
		AgentID:    agentID,
		TotalScore: total,
		Dimensions: dims,
	}, nil
}

// ===== 已有 calc 函数(W1 维持) =====

// calcSkillMatch 计算技能匹配度
func (e *ScoringEngine) calcSkillMatch(agentSkills []string, required []string) float64 {
	if len(required) == 0 {
		return 1.0
	}

	match := 0
	for _, req := range required {
		for _, skill := range agentSkills {
			if skill == req {
				match++
				break
			}
		}
	}

	return float64(match) / float64(len(required))
}

// calcHealthScore 计算健康分数
func (e *ScoringEngine) calcHealthScore(load *registry.AgentLoad) float64 {
	if load == nil {
		return 0.5
	}

	// CPU和内存使用率越低越健康
	cpuScore := 1.0 - load.CPUUsage
	memScore := 1.0 - load.MemoryUsage

	return (cpuScore + memScore) / 2
}

// calcLoadScore 计算负载分数
func (e *ScoringEngine) calcLoadScore(load *registry.AgentLoad) float64 {
	if load == nil || load.MaxCapacity == 0 {
		return 0.5
	}

	loadRate := float64(load.ActiveTasks) / float64(load.MaxCapacity)
	return 1.0 - loadRate
}

// calcNetworkPenalty 计算网络惩罚
func (e *ScoringEngine) calcNetworkPenalty(agentUniverse, sourceUniverse string) float64 {
	if sourceUniverse == "" {
		return 1.0
	}

	if agentUniverse == sourceUniverse {
		return 1.0 // 同Universe，无惩罚
	}

	return 0.7 // 跨Universe惩罚
}

// ===== v0.7.0 W1: 7 维 dimension 函数(走 DataSource) =====
//
// 设计原则:
// 1. 如果 ds == nil (老调用方式),fallback 到 v0.6.0 占位值(向后兼容)
// 2. 如果 ds != nil,call ds.xxxScore() 真算
// 3. 真算失败 → 用 v0.6.0 占位值 + 记日志(降级而非崩溃)

func (e *ScoringEngine) dimensionTrustScore(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 0.5 // v0.6.0 占位
	}
	v, err := e.ds.TrustScore(ctx, agentID)
	if err != nil {
		e.logger.Warn("TrustScore failed", "agent", agentID, "err", err)
		return 0.5
	}
	return v
}

func (e *ScoringEngine) dimensionLatencyScore(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 1.0 // v0.6.0 占位
	}
	v, err := e.ds.LatencyScore(ctx, agentID)
	if err != nil {
		e.logger.Warn("LatencyScore failed", "agent", agentID, "err", err)
		return 1.0
	}
	return v
}

func (e *ScoringEngine) dimensionSuccessRate(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 0.5 // v0.6.0 占位
	}
	v, err := e.ds.SuccessRate(ctx, agentID)
	if err != nil {
		e.logger.Warn("SuccessRate failed", "agent", agentID, "err", err)
		return 0.5
	}
	return v
}

func (e *ScoringEngine) dimensionBandwidthScore(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 1.0 // v0.6.0 占位
	}
	v, err := e.ds.BandwidthScore(ctx, agentID)
	if err != nil {
		e.logger.Warn("BandwidthScore failed", "agent", agentID, "err", err)
		return 1.0
	}
	return v
}

func (e *ScoringEngine) dimensionAuthLevel(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 1.0 // v0.6.0 占位
	}
	meta, err := e.ds.GetMeta(ctx, agentID)
	if err != nil {
		e.logger.Warn("AuthLevel GetMeta failed", "agent", agentID, "err", err)
		return 1.0
	}
	return meta.AuthLevel
}

func (e *ScoringEngine) dimensionProtocolCompat(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 1.0 // v0.6.0 占位
	}
	meta, err := e.ds.GetMeta(ctx, agentID)
	if err != nil {
		e.logger.Warn("ProtocolCompat GetMeta failed", "agent", agentID, "err", err)
		return 1.0
	}
	if len(meta.SupportedInterfaces) == 0 {
		return 0.0
	}
	// Intersection of kernel preferred ∩ agent supported
	preferred := map[string]bool{}
	// SourceRegion source: e.ds.config (via type assertion)
	dsVal, ok := e.ds.(*RedisDataSource)
	if !ok {
		// Unknown impl: assume both A2A and AFP are preferred
		preferred["A2A"] = true
		preferred["AFP"] = true
	} else {
		for _, p := range dsVal.config.PreferredProtocols {
			preferred[p] = true
		}
	}
	match := 0
	for _, p := range meta.SupportedInterfaces {
		if preferred[p] {
			match++
		}
	}
	if match == 0 {
		return 0.0
	}
	return float64(match) / float64(len(preferred))
}

func (e *ScoringEngine) dimensionHistoryCount(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 0.5 // v0.6.0 占位
	}
	v, err := e.ds.HistoryCount(ctx, agentID)
	if err != nil {
		e.logger.Warn("HistoryCount failed", "agent", agentID, "err", err)
		return 0.5
	}
	return v
}

func (e *ScoringEngine) dimensionErrorRate(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 1.0 // v0.6.0 占位
	}
	v, err := e.ds.ErrorRate(ctx, agentID)
	if err != nil {
		e.logger.Warn("ErrorRate failed", "agent", agentID, "err", err)
		return 1.0
	}
	return v
}

// ===== v0.7.0 W2: 4 维占位(待 W2 末真算) =====

func (e *ScoringEngine) dimensionAvailability(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 1.0
	}
	v, err := e.ds.Availability(ctx, agentID)
	if err != nil {
		e.logger.Warn("Availability failed", "agent", agentID, "err", err)
		return 1.0
	}
	return v
}

func (e *ScoringEngine) dimensionVersionCompat(ctx context.Context, agentID string) float64 {
	if e.ds == nil {
		return 1.0
	}
	v, err := e.ds.VersionCompat(ctx, agentID)
	if err != nil {
		e.logger.Warn("VersionCompat failed", "agent", agentID, "err", err)
		return 1.0
	}
	return v
}

func (e *ScoringEngine) dimensionGeoPenalty(ctx context.Context, agentID string, sourceRegion string) float64 {
	if e.ds == nil {
		return 1.0
	}
	v, err := e.ds.GeoPenalty(ctx, agentID, sourceRegion)
	if err != nil {
		e.logger.Warn("GeoPenalty failed", "agent", agentID, "err", err)
		return 1.0
	}
	return v
}

package scheduler

import (
	"context"
	"log/slog"
	"sync"

	"github.com/wau/registry/registry"
)

// ScoringEngine 评分引擎 (线程安全)
type ScoringEngine struct {
	reg     registry.Registry
	weights Weights
	logger  *slog.Logger
	mu      sync.RWMutex // 保护并发访问
}

// NewScoringEngine 创建评分引擎
func NewScoringEngine(reg registry.Registry, logger *slog.Logger) *ScoringEngine {
	return &ScoringEngine{
		reg:     reg,
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
		// 1. Skill匹配度 (0-1)
		SkillMatch: e.calcSkillMatch(card.Skills, req.RequiredSkills),

		// 2. Trust分数 (0-1) - 默认0.5，需要Trust模块支持
		TrustScore: 0.5,

		// 3. 健康状态 (0-1): 基于CPU和内存使用率
		HealthScore: e.calcHealthScore(load),

		// 4. 延迟分数 (0-1): 默认满分
		LatencyScore: 1.0,

		// 5. 负载分数 (0-1): 负载越低分数越高
		LoadScore: e.calcLoadScore(load),

		// 6. 成功率 (0-1): 默认0.5
		SuccessRate: 0.5,

		// 7. 网络惩罚 (0.7-1.0): 跨Universe降低
		NetworkPenalty: e.calcNetworkPenalty(card.Universe, req.SourceUniverse),

		// 8. 带宽可用性 (0-1): 默认满分
		BandwidthScore: 1.0,

		// 9. 认证级别 (0-1): 默认满分
		AuthLevel: 1.0,

		// 10. 协议兼容性 (0-1): 默认满分
		ProtocolCompat: 1.0,

		// 11. 历史交互次数 (0-1): 默认0.5
		HistoryCount: 0.5,

		// 12. 错误率 (0-1): 默认满分
		ErrorRate: 1.0,

		// 13. 可用性 (0-1): 默认满分
		Availability: 1.0,

		// 14. 版本兼容性 (0-1): 默认满分
		VersionCompat: 1.0,

		// 15. 地理位置惩罚 (0.9-1.0): 默认无惩罚
		GeoPenalty: 1.0,
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

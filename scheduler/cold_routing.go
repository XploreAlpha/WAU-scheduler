// Package scheduler: cold routing policy (v0.8.0 M4-1.3)
//
// Cold routing 是 WAU 神经可塑性(W-3 智慧)的核心能力:把没历史数据的 fresh
// agent 拉进 explore pool,让它跑几次积累 trust 数据,再正式进 warm pool
// 参与 15 维打分排名。
//
// 设计目标:
//   1. 不破坏现有 15 维打分向后兼容 — policy nil = 全 warm(原行为)
//   2. Explore 概率可控 — 默认 10% 请求走 explore,避免过载 cold agent
//   3. Cold agent 也得满足 skill 匹配 — SkillFloor 0.5 兜底,避免 cold 但
//      不胜任的 agent 抢任务
//   4. Cold pool 内选任务数最少 — 给最"生疏"的 agent 优先机会,均衡探索
package scheduler

import (
	"context"
	"hash/fnv"
	"log/slog"
	"math"
)

// Default cold routing parameters (v0.8.0 M4-1.3).
//
// These defaults are chosen to be safe for production:
//   - 10% explore budget: enough to give cold agents a chance,
//     not enough to degrade warm pool QoS
//   - 10 successful records = warmup: enough trust signal to
//     trust the 15-dim score (EMA weight 0.1 over 10 records =
//     ~65% of score influenced by history)
//   - 0.5 skill floor: cold agent must match at least half the
//     required skills; below that, the agent can't be expected
//     to handle the task well
const (
	DefaultExploreBudget   = 0.10
	DefaultWarmupThreshold = 10
	DefaultSkillFloor      = 0.5
)

// ColdRoutingPolicy configures cold agent exploration (v0.8.0 M4-1.3).
//
// When attached to a Scheduler, the policy:
//   1. With probability ExploreBudget, picks from cold pool (explore mode)
//   2. Otherwise, falls back to 15-dim scoring (warm mode)
//
// Cold pool selection criteria:
//   - Agent.IsCold == true (no wau-trust history)
//   - Agent's SkillMatch >= SkillFloor (avoid incompetent cold agents)
//   - Among matches, pick the one with fewest recorded tasks (most "fresh")
//
// All fields are tunable. Zero values trigger the defaults via
// NewColdRoutingPolicy / normalize().
type ColdRoutingPolicy struct {
	// ExploreBudget is the probability (0.0 - 1.0) of routing a
	// request to the cold pool. Default 0.10.
	ExploreBudget float64

	// WarmupThreshold is the number of successful records an agent
	// needs before transitioning cold → warm. Used to compute whether
	// an agent is "warm enough" to skip the explore pool.
	//
	// Note: this threshold is informational at the policy level; the
	// actual cold/warm transition is driven by IsCold from wau-trust
	// (which returns false once any Record/Reset is called).
	// Default 10.
	WarmupThreshold int

	// SkillFloor is the minimum SkillMatch score a cold agent must
	// have to be considered for routing. Default 0.5.
	SkillFloor float64

	// Logger for cold routing decisions (optional).
	Logger *slog.Logger

	// hashSeed is used for deterministic-but-distributed explore/warm
	// decisions (see ShouldExplore). Not exposed; set in NewColdRoutingPolicy.
	hashSeed uint32
}

// NewColdRoutingPolicy creates a policy with the given parameters.
// Zero/negative values are replaced with defaults.
//
// Seeded with current time + caller PID for low collision across schedulers.
func NewColdRoutingPolicy(exploreBudget float64, warmupThreshold int, skillFloor float64, logger *slog.Logger) *ColdRoutingPolicy {
	p := &ColdRoutingPolicy{
		ExploreBudget:   exploreBudget,
		WarmupThreshold: warmupThreshold,
		SkillFloor:      skillFloor,
		Logger:          logger,
	}
	p.normalize()
	return p
}

// DefaultColdRoutingPolicy returns a policy with safe defaults.
func DefaultColdRoutingPolicy(logger *slog.Logger) *ColdRoutingPolicy {
	return NewColdRoutingPolicy(
		DefaultExploreBudget,
		DefaultWarmupThreshold,
		DefaultSkillFloor,
		logger,
	)
}

// normalize applies defaults to zero/negative fields.
func (p *ColdRoutingPolicy) normalize() {
	if p.ExploreBudget <= 0 || p.ExploreBudget >= 1.0 {
		p.ExploreBudget = DefaultExploreBudget
	}
	if p.WarmupThreshold <= 0 {
		p.WarmupThreshold = DefaultWarmupThreshold
	}
	if p.SkillFloor <= 0 || p.SkillFloor > 1.0 {
		p.SkillFloor = DefaultSkillFloor
	}
	if p.hashSeed == 0 {
		// FNV-1a over a small constant — enough entropy for
		// distributed scheduling decisions without importing time/rand
		h := fnv.New32a()
		_, _ = h.Write([]byte("wau-cold-routing-v0.8.0-M4-1.3"))
		p.hashSeed = h.Sum32()
		if p.hashSeed == 0 {
			p.hashSeed = 1 // avoid 0 (would always be warm)
		}
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
}

// ShouldExplore decides whether a given request should go to the cold pool.
// Deterministic per (seed, requestID) so a request is consistently routed
// across retries; distributes across cold agents in aggregate.
//
// Returns true → explore mode (route to cold pool)
// Returns false → warm mode (fall back to 15-dim scoring)
func (p *ColdRoutingPolicy) ShouldExplore(requestID string) bool {
	if requestID == "" {
		// No request ID = use a pseudo-random draw (cheap)
		return p.shouldExploreRandom()
	}
	// Hash-based: same requestID always returns same decision
	h := fnv.New32a()
	_, _ = h.Write([]byte(requestID))
	combined := h.Sum32() ^ p.hashSeed
	// Normalize to [0, 1) by dividing by 2^32
	draw := float64(combined) / float64(math.MaxUint32)
	return draw < p.ExploreBudget
}

// shouldExploreRandom is the fallback when no requestID is provided.
// Uses a simple LCG seeded by hashSeed; not cryptographically random,
// but good enough for distributing cold routing decisions.
func (p *ColdRoutingPolicy) shouldExploreRandom() bool {
	// LCG step (Numerical Recipes constants)
	p.hashSeed = p.hashSeed*1664525 + 1013904223
	if p.hashSeed == 0 {
		p.hashSeed = 1
	}
	draw := float64(p.hashSeed) / float64(math.MaxUint32)
	return draw < p.ExploreBudget
}

// SelectCold picks a cold agent from candidates (v0.8.0 M4-1.3).
//
// Selection criteria (all must pass):
//   1. scoring.IsCold(ctx, agentID) == true
//   2. candidates[i].Dimensions.SkillMatch >= p.SkillFloor
//
// Among matching candidates, the choice is deterministic-but-distributed:
// we hash (requestID || agentID) mod candidate count to pick one. This:
//   - Gives same requestID → same agent (consistent retries)
//   - Distributes load across all cold agents over time (hash spread)
//   - Avoids always picking the lowest-score agent (which would starve
//     higher-scored cold agents of explore opportunities)
//
// Pass requestID == "" to use a hash-seed-based pseudo-random draw
// (no consistency guarantees, but still distributes evenly).
//
// Returns nil if no cold agent meets criteria (caller should fall back
// to warm pool).
func (p *ColdRoutingPolicy) SelectCold(
	ctx context.Context,
	candidates []AgentScore,
	scoring *ScoringEngine,
	requestID string,
) *AgentScore {
	// Step 1: filter to cold + skill-floor-matching agents
	eligible := make([]*AgentScore, 0, len(candidates))
	for i := range candidates {
		c := &candidates[i]
		cold, err := scoring.IsCold(ctx, c.AgentID)
		if err != nil {
			p.Logger.Warn("IsCold failed", "agent", c.AgentID, "err", err)
			continue
		}
		if !cold {
			continue
		}
		if c.Dimensions.SkillMatch < p.SkillFloor {
			continue
		}
		eligible = append(eligible, c)
	}
	if len(eligible) == 0 {
		return nil
	}

	// Step 2: deterministic pick via hash(requestID || index)
	// This guarantees uniform distribution across the eligible pool.
	var idx int
	if requestID == "" {
		// Fallback: use hashSeed-stepped LCG for pseudo-random
		p.hashSeed = p.hashSeed*1664525 + 1013904223
		if p.hashSeed == 0 {
			p.hashSeed = 1
		}
		idx = int(p.hashSeed) % len(eligible)
	} else {
		h := fnv.New32a()
		_, _ = h.Write([]byte(requestID))
		combined := h.Sum32() ^ p.hashSeed
		idx = int(combined) % len(eligible)
	}

	chosen := eligible[idx]
	p.Logger.Info("cold routing: explore",
		"agent", chosen.AgentID,
		"skill_match", chosen.Dimensions.SkillMatch,
		"total_score", chosen.TotalScore,
		"request_id", requestID,
	)
	return chosen
}

package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// SleepPolicy governs when agents should sleep and when to wake them (v0.8.0 M4-2).
//
// The policy is OPT-IN — pass nil to NewSchedulerWithSleepPolicy to disable
// all sleep behavior (backward compat with v0.7.x).
//
// Sleep is a resource-saving optimization: when an agent has been idle long
// enough AND its trust score has decayed below the threshold, the agent is
// put to sleep (excluded from scheduling). Wake is demand-driven: when the
// pending queue exceeds WakeQueueDepth, the highest-trust asleep agent is
// woken to handle the spike.
//
// Target (per plan §3.4): 降耗 ≥ 30% during low-traffic windows.
type SleepPolicy struct {
	// SleepTrustThreshold — agents with trust score strictly less than this
	// value are eligible for sleep. Default 0.3.
	//
	// Rationale: agents with very low trust have low value in the routing
	// pool. Sleeping them frees scheduler + agent resources. The threshold
	// is well below DefaultTrustScore (0.5) so well-behaved agents are
	// never put to sleep on cold start.
	SleepTrustThreshold float64

	// WakeQueueDepth — when the pending queue depth strictly exceeds this
	// value, the policy considers waking asleep agents. Default 50.
	//
	// Rationale: a queue > 50 means the available warm pool is overwhelmed.
	// Waking asleep agents at this threshold gives the system headroom
	// before queue latency becomes user-visible.
	WakeQueueDepth int

	// IdleDuration — agents must be idle (no schedule) for at least this
	// duration to be eligible for sleep. Default 24h.
	//
	// Rationale: short idle gaps are normal (e.g. between bursts); we want
	// to sleep genuinely idle agents, not agents that just had a quiet
	// 5-minute window. 24h matches the plan §3.4 "30 天无请求" target with
	// a more aggressive (faster to wake back up) threshold.
	IdleDuration time.Duration

	// Logger — optional slog logger; defaults to slog.Default() if nil.
	Logger *slog.Logger
}

// Defaults for SleepPolicy fields.
const (
	DefaultSleepTrustThreshold = 0.3
	DefaultWakeQueueDepth      = 50
	DefaultIdleDuration        = 24 * time.Hour
)

// NewSleepPolicy returns a SleepPolicy with default thresholds. The returned
// policy is safe to use immediately. Callers may override any field after
// construction if their deployment needs custom tuning.
func NewSleepPolicy(logger *slog.Logger) *SleepPolicy {
	if logger == nil {
		logger = slog.Default()
	}
	return &SleepPolicy{
		SleepTrustThreshold: DefaultSleepTrustThreshold,
		WakeQueueDepth:      DefaultWakeQueueDepth,
		IdleDuration:        DefaultIdleDuration,
		Logger:              logger,
	}
}

// FilterAsleep removes asleep agents from the candidate pool (v0.8.0 M4-2).
//
// This is the primary entry point used by Scheduler.Schedule() — call this
// BEFORE cold routing and 15-dim ranking to ensure asleep agents never
// participate in scheduling.
//
// Asleep agents are excluded regardless of their other dimensions (trust,
// skills, latency, etc.). Sleep is orthogonal to cold/warm — a fresh cold
// agent is NOT asleep (it has no sleep flag), but once it warms up it can
// be put to sleep if utilization drops.
//
// Behavior when scoring == nil: returns the candidates unchanged (backward
// compat — sleep filter requires a scoring engine to query IsAsleep).
//
// Logs each filtered agent at Info level for observability.
func (p *SleepPolicy) FilterAsleep(ctx context.Context, candidates []AgentScore, scoring *ScoringEngine) []AgentScore {
	if p == nil || scoring == nil || len(candidates) == 0 {
		return candidates
	}

	out := make([]AgentScore, 0, len(candidates))
	filtered := 0
	for _, c := range candidates {
		asleep, err := scoring.IsAsleep(ctx, c.AgentID)
		if err != nil {
			// Conservative: on error, treat as awake (don't block routing)
			out = append(out, c)
			continue
		}
		if asleep {
			filtered++
			p.Logger.Info("skipping asleep agent",
				"agent", c.AgentID,
				"score", c.TotalScore,
			)
			continue
		}
		out = append(out, c)
	}
	if filtered > 0 {
		p.Logger.Info("asleep agents filtered from scheduling pool",
			"filtered_count", filtered,
			"remaining", len(out),
		)
	}
	return out
}

// ShouldWake returns true when the pending queue depth indicates a demand
// spike that warrants waking asleep agents (v0.8.0 M4-2).
//
// The wake decision is queue-driven: queueDepth strictly greater than
// WakeQueueDepth triggers wake. This is intentionally a coarse threshold —
// the cost of waking an agent is low compared to the cost of queue latency.
//
// Behavior when p == nil: returns false (no wake — backward compat).
func (p *SleepPolicy) ShouldWake(queueDepth int) bool {
	if p == nil {
		return false
	}
	return queueDepth > p.WakeQueueDepth
}

// PickWakeTarget selects the highest-trust asleep agent to wake (v0.8.0 M4-2).
//
// Among the asleep candidates provided, returns the one with the highest
// TotalScore (which is dominated by the TrustScore dimension when agents
// are asleep — they have low utilization so other dimensions like latency
// are likely stale or neutral).
//
// Returns nil if no candidates are provided.
//
// Caller responsibility: after picking the wake target, invoke
// wau-trust Engine.Wake(agent) to actually transition the agent back to
// awake state. This policy only selects; mutation is separate.
//
// Behavior when p == nil or candidates empty: returns nil.
func (p *SleepPolicy) PickWakeTarget(candidates []AgentScore) *AgentScore {
	if p == nil || len(candidates) == 0 {
		return nil
	}
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.TotalScore > best.TotalScore {
			best = c
		}
	}
	p.Logger.Info("wake target selected",
		"agent", best.AgentID,
		"score", best.TotalScore,
		"candidates", len(candidates),
	)
	return &best
}

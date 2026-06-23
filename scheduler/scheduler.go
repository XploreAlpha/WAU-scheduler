package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// Scheduler 任务调度器
type Scheduler struct {
	reg     registry.Registry
	scoring *ScoringEngine
	logger  *slog.Logger

	// coldPolicy is the cold routing policy (v0.8.0 M4-1.4).
	// nil = 现有行为(全 warm 池,15 维打分,向后兼容 v0.7.x)
	coldPolicy *ColdRoutingPolicy

	// sleepPolicy is the sleep/wake policy (v0.8.0 M4-2.2).
	// nil = 现有行为(不过滤 asleep agents,向后兼容 v0.7.x + M4-1.4)
	sleepPolicy *SleepPolicy

	// replicationPolicy is the self-replication policy (v0.8.0 M4-3.2).
	// nil = 现有行为(无 Replicate 决策,向后兼容 v0.7.x + M4-1.4 + M4-2.2)
	replicationPolicy *ReplicationPolicy

	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
}

// NewScheduler 创建调度器(无 cold policy,向后兼容 v0.7.x)
func NewScheduler(reg *registry.RedisStore, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		reg:               reg,
		scoring:           NewScoringEngine(reg, logger),
		logger:            logger,
		coldPolicy:        nil,
		sleepPolicy:       nil,
		replicationPolicy: nil, // v0.8.0 M4-3.2: disabled by default
		stopCh:            make(chan struct{}),
	}
}

// NewSchedulerWithRegistry 创建调度器,接受 registry.Registry interface
//
// v0.8.0 M4-1.4:reg 字段从具体类型 *registry.RedisStore 改为 interface,
// 便于测试用 mock(集成 cold policy 时需要 mock registry)。
// 生产仍传 *registry.RedisStore(它实现了 registry.Registry interface)。
func NewSchedulerWithRegistry(reg registry.Registry, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		reg:               reg,
		scoring:           NewScoringEngine(reg, logger),
		logger:            logger,
		coldPolicy:        nil,
		sleepPolicy:       nil,
		replicationPolicy: nil, // v0.8.0 M4-3.2: disabled by default
		stopCh:            make(chan struct{}),
	}
}

// NewSchedulerWithColdPolicy 创建调度器 + cold routing policy (v0.8.0 M4-1.4)
//
// coldPolicy == nil 等价 NewSchedulerWithRegistry(reg, logger)
//
// 用法:
//
//	policy := scheduler.NewColdRoutingPolicy(0.10, 10, 0.5, logger)
//	s := scheduler.NewSchedulerWithColdPolicy(reg, logger, policy)
//	result, _ := s.Schedule(ctx, req)  // 10% 概率走 cold explore
func NewSchedulerWithColdPolicy(reg registry.Registry, logger *slog.Logger, coldPolicy *ColdRoutingPolicy) *Scheduler {
	return &Scheduler{
		reg:               reg,
		scoring:           NewScoringEngine(reg, logger),
		logger:            logger,
		coldPolicy:        coldPolicy,
		sleepPolicy:       nil,
		replicationPolicy: nil, // v0.8.0 M4-3.2: disabled by default
		stopCh:            make(chan struct{}),
	}
}

// NewSchedulerWithSleepPolicy creates a scheduler with BOTH cold and sleep
// policies enabled (v0.8.0 M4-2.2).
//
// This is the recommended constructor for v0.8.0 production deployments
// that want the full W-3 智慧 initial set (cold routing + sleep/wake).
//
// Either policy may be nil — both are optional. nil cold policy = no
// cold routing (15-dim ranking only); nil sleep policy = no sleep filter
// (all agents considered, regardless of awake/asleep state).
//
// Usage:
//
//	coldPolicy := scheduler.NewColdRoutingPolicy(0.10, 10, 0.5, logger)
//	sleepPolicy := scheduler.NewSleepPolicy(logger)
//	s := scheduler.NewSchedulerWithSleepPolicy(reg, logger, coldPolicy, sleepPolicy)
//	result, _ := s.Schedule(ctx, req)
func NewSchedulerWithSleepPolicy(reg registry.Registry, logger *slog.Logger, coldPolicy *ColdRoutingPolicy, sleepPolicy *SleepPolicy) *Scheduler {
	return &Scheduler{
		reg:               reg,
		scoring:           NewScoringEngine(reg, logger),
		logger:            logger,
		coldPolicy:        coldPolicy,
		sleepPolicy:       sleepPolicy,
		replicationPolicy: nil, // v0.8.0 M4-3.2: disabled by default
		stopCh:            make(chan struct{}),
	}
}

// NewSchedulerWithReplicationPolicy creates a scheduler with cold + sleep +
// replication policies enabled (v0.8.0 M4-3.2).
//
// All three policies are optional. This is the recommended constructor for
// v0.8.0 production deployments that want the full W-3 智慧 initial set:
// cold routing (M4-1) + sleep/wake (M4-2) + self-replication (M4-3).
//
// Usage:
//
//	coldPolicy := scheduler.NewColdRoutingPolicy(0.10, 10, 0.5, logger)
//	sleepPolicy := scheduler.NewSleepPolicy(logger)
//	replicationPolicy := scheduler.NewReplicationPolicy(logger)
//	s := scheduler.NewSchedulerWithReplicationPolicy(reg, logger, coldPolicy, sleepPolicy, replicationPolicy)
//	decision, _ := s.Replicate(ctx, "parent-agent", "child-agent")
func NewSchedulerWithReplicationPolicy(
	reg registry.Registry,
	logger *slog.Logger,
	coldPolicy *ColdRoutingPolicy,
	sleepPolicy *SleepPolicy,
	replicationPolicy *ReplicationPolicy,
) *Scheduler {
	return &Scheduler{
		reg:               reg,
		scoring:           NewScoringEngine(reg, logger),
		logger:            logger,
		coldPolicy:        coldPolicy,
		sleepPolicy:       sleepPolicy,
		replicationPolicy: replicationPolicy,
		stopCh:            make(chan struct{}),
	}
}

// NewSchedulerWithScoring creates a scheduler with a custom ScoringEngine
// (v0.8.0 M4-2.2).
//
// Useful for tests that need a custom DataSource (e.g. WauTrustDataSource
// wrapping MemoryDataSource + MemoryEngine). Production code typically uses
// NewScheduler / NewSchedulerWithRegistry which build the default
// ScoringEngine internally.
func NewSchedulerWithScoring(reg registry.Registry, scoring *ScoringEngine, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		reg:               reg,
		scoring:           scoring,
		logger:            logger,
		replicationPolicy: nil, // v0.8.0 M4-3.2: replication disabled (tests add via SetReplicationPolicy)
		stopCh:            make(chan struct{}),
	}
}

// SetColdPolicy sets or replaces the cold routing policy (v0.8.0 M4-1.4).
//
// Useful for dynamic policy updates without recreating the scheduler.
// Pass nil to disable cold routing (backward-compat with v0.7.x).
func (s *Scheduler) SetColdPolicy(policy *ColdRoutingPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.coldPolicy = policy
}

// SetSleepPolicy sets or replaces the sleep policy (v0.8.0 M4-2.2).
//
// Useful for dynamic policy updates without recreating the scheduler.
// Pass nil to disable sleep filtering (backward-compat with v0.7.x + M4-1.4).
func (s *Scheduler) SetSleepPolicy(policy *SleepPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sleepPolicy = policy
}

// SetReplicationPolicy sets or replaces the replication policy (v0.8.0 M4-3.2).
//
// Useful for dynamic policy updates without recreating the scheduler.
// Pass nil to disable replication (backward-compat with v0.7.x + M4-1.4 + M4-2.2).
//
// The kernel (WAU-core-kernel M4-3.3) holds a reference to the same policy
// instance for the post-success RecordChild call, so swapping the policy here
// also affects kernel bookkeeping (kernel should be re-initialized).
func (s *Scheduler) SetReplicationPolicy(policy *ReplicationPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicationPolicy = policy
}

// GetReplicationPolicy returns the current replication policy (may be nil).
//
// Convenience accessor for kernel code that needs to call RecordChild after
// successful engine.Replicate + registry.Heartbeat.
func (s *Scheduler) GetReplicationPolicy() *ReplicationPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.replicationPolicy
}

// Replicate decides whether parent agent may spawn child, returning a
// ReplicateDecision struct (v0.8.0 M4-3.2).
//
// LIBRARY BOUNDARY: Replicate performs NO writes. It is a pure decision
// function that returns either (nil, error) on policy violation or
// (*ReplicateDecision, nil) on success. The caller (typically WAU-core-kernel
// M4-3.3) reads the decision and executes:
//
//	decision, err := s.Replicate(ctx, parentID, childID)
//	if err != nil { return err }                       // policy violation
//	trustEngine.Replicate(ctx, decision.ParentID,
//	                       decision.ChildID,
//	                       decision.InheritanceFactor)   // writes child trust
//	registry.Heartbeat(ctx, child metadata from decision.ChildSpec)
//	policy := s.GetReplicationPolicy()
//	if policy != nil { policy.RecordChild(decision.ParentID) } // rate-limit counter
//
// Decision flow:
//  1. replicationPolicy == nil       → ErrPolicyDisabled
//  2. parentID == "" or childID == "" → ErrInvalidChildID
//  3. parent not in registry          → ErrParentNotFound
//  4. scoring.IsCold(parent) == true  → ErrParentCold
//  5. policy.CanReplicate(trust, count) → ErrParentLowTrust / ErrChildLimitReached
//  6. Build child spec (from parent card)
//  7. Pre-compute expected child trust via engine.ReplicateTrust (pure helper)
//  8. Return ReplicateDecision
//
// The decision is point-in-time; retries should re-call Replicate. Concurrent
// decisions may differ if state changes between calls.
func (s *Scheduler) Replicate(ctx context.Context, parentID, childID string) (*ReplicateDecision, error) {
	s.mu.RLock()
	policy := s.replicationPolicy
	s.mu.RUnlock()

	// 1. Policy disabled
	if policy == nil {
		return nil, ErrPolicyDisabled
	}

	// 2. Validate IDs
	if parentID == "" || childID == "" {
		return nil, ErrInvalidChildID
	}

	// 3. Parent exists in registry.
	//    GetAgent may return (nil, err) for not-found, or (nil, nil) for
	//    "agent missing but no error". Both are treated as ErrParentNotFound.
	//    Other errors are wrapped with context.
	parent, err := s.reg.GetAgent(ctx, parentID)
	if err != nil {
		if parent == nil {
			return nil, ErrParentNotFound
		}
		return nil, fmt.Errorf("replicate parent lookup %s: %w", parentID, err)
	}
	if parent == nil {
		return nil, ErrParentNotFound
	}

	// 4. Parent is cold (no trust data) — check BEFORE trust threshold
	//    so cold parents get the more diagnostic error.
	cold, coldErr := s.scoring.IsCold(ctx, parentID)
	if coldErr != nil {
		s.logger.Warn("IsCold failed during Replicate", "parent", parentID, "err", coldErr)
		// Conservative: don't block on transient error, continue to trust check
	} else if cold {
		return nil, ErrParentCold
	}

	// 5. Trust + child count check
	parentTrust, _ := s.scoring.TrustScore(ctx, parentID)
	currentChildren := policy.ChildCount(parentID)
	if err := policy.CanReplicate(parentTrust, currentChildren); err != nil {
		s.logger.Info("replication denied by policy",
			"parent", parentID,
			"trust", parentTrust,
			"children", currentChildren,
			"err", err.Error(),
		)
		return nil, err
	}

	// 6. Build child spec + expected trust
	spec := policy.BuildChildSpec(parent, childID)
	expectedTrust := engine.ReplicateTrust(parentTrust, policy.InheritanceFactor, parentID, childID)

	// 7. Return decision
	decision := &ReplicateDecision{
		ParentID:           parentID,
		ParentTrust:        parentTrust,
		CurrentChildren:    currentChildren,
		ChildID:            childID,
		InheritanceFactor:  policy.InheritanceFactor,
		ExpectedChildTrust: expectedTrust,
		ChildSpec:          spec,
		Rationale:          policy.RationaleFor(parentID, parentTrust, currentChildren),
	}
	s.logger.Info("replication decision: allowed",
		"parent", parentID,
		"child", childID,
		"expected_trust", expectedTrust,
		"factor", policy.InheritanceFactor,
		"current_children", currentChildren,
	)
	return decision, nil
}

// Schedule 调度任务 - 选择最佳Agent
//
// v0.8.0 M4-2.2 完整决策链:
//   1. 获取在线 agents
//   2. 15 维打分
//   3. sleepPolicy != nil → 过滤 asleep agents(永远不参与 ranking)
//   4. coldPolicy != nil → ShouldExplore + SelectCold 决策
//   5. warm 路径 → 取最高分
//   6. 无候选 → fallback warm / ErrNoAvailableAgent
//
// 否则(nil policies)走原 15 维打分,完全向后兼容。
func (s *Scheduler) Schedule(ctx context.Context, req *ScheduleRequest) (*ScheduleResult, error) {
	// 1. 获取在线Agent
	agents, err := s.reg.GetOnlineAgents(ctx)
	if err != nil {
		return nil, err
	}

	if len(agents) == 0 {
		return nil, ErrNoAvailableAgent
	}

	agentIDs := make([]string, len(agents))
	for i, agent := range agents {
		agentIDs[i] = agent.ID
	}

	// 2. 构建评分请求
	scoreReq := &ScoreRequest{
		RequiredSkills: req.RequiredSkills,
		IntentType:     req.IntentType,
		Urgency:        req.Urgency,
		SourceUniverse: req.SourceUniverse,
	}

	// 3. 评分
	scores, err := s.scoring.ScoreAgents(ctx, agentIDs, scoreReq)
	if err != nil {
		return nil, err
	}

	if len(scores) == 0 {
		return nil, ErrNoAvailableAgent
	}

	// 4. 选择最佳Agent(asleep 过滤 → cold routing → warm fallback)
	reqID := ""
	if req.Task != nil {
		reqID = req.Task.TaskID
	}
	best, coldRouted := s.pickBest(ctx, scores, reqID)

	result := &ScheduleResult{
		Task:         req.Task,
		AgentID:      best.AgentID,
		Score:        best.TotalScore,
		DispatchedAt: time.Now(),
	}

	// 5. 更新任务状态
	if req.Task != nil {
		req.Task.Status = TaskStatusDispatched
		req.Task.AssignedAgent = best.AgentID
		req.Task.UpdatedAt = time.Now().Unix()
	}

	s.logger.Info("Task scheduled",
		"task", req.Task.TaskID,
		"agent", best.AgentID,
		"score", best.TotalScore,
		"cold_routed", coldRouted,
	)

	return result, nil
}

// pickBest applies sleep filter + cold policy (if set) to choose among
// scored candidates.
//
// Decision chain (v0.8.0 M4-2.2):
//   1. sleepPolicy.FilterAsleep → remove asleep agents from pool
//   2. if all filtered out → return ErrNoAvailableAgent
//   3. coldPolicy.ShouldExplore + SelectCold → cold explore path
//   4. fallback: highest-score warm agent
//
// Returns (chosen, coldRouted). coldRouted=true means the choice came
// from the cold explore pool rather than the warm 15-dim ranking.
func (s *Scheduler) pickBest(ctx context.Context, scores []AgentScore, requestID string) (AgentScore, bool) {
	s.mu.RLock()
	policy := s.coldPolicy
	sleep := s.sleepPolicy
	s.mu.RUnlock()

	// Step 1: filter asleep agents (v0.8.0 M4-2.2)
	if sleep != nil {
		scores = sleep.FilterAsleep(ctx, scores, s.scoring)
		if len(scores) == 0 {
			// All agents asleep — surface this as a routing failure so the
			// caller can decide whether to trigger wake (or just retry later).
			s.logger.Warn("all agents asleep, no eligible agent for routing",
				"request_id", requestID,
			)
			// Return the highest-score asleep agent with coldRouted=false so
			// the schedule can still proceed (caller may decide to wake it).
			return AgentScore{}, false
		}
	}

	// Step 2: cold routing path (v0.8.0 M4-1.4)
	if policy == nil {
		// No cold policy → highest score (legacy v0.7.x behavior)
		return scores[0], false
	}

	// Cold routing path
	if policy.ShouldExplore(requestID) {
		if cold := policy.SelectCold(ctx, scores, s.scoring, requestID); cold != nil {
			return *cold, true
		}
		// No cold agent met criteria → fall back to warm pool
		s.logger.Debug("cold explore found no eligible agent, falling back to warm", "request_id", requestID)
	}
	return scores[0], false
}

// ScheduleSimple 简化调度 - 输入所需技能列表，返回最佳Agent
//
// v0.8.0 M4-1.4:如果设置了 coldPolicy,同样应用 cold 决策(用空 requestID,
// 走 LCG fallback 决定 explore/warm)
func (s *Scheduler) ScheduleSimple(ctx context.Context, requiredSkills []string) (*AgentScore, error) {
	agents, err := s.reg.GetOnlineAgents(ctx)
	if err != nil {
		return nil, err
	}

	if len(agents) == 0 {
		return nil, ErrNoAvailableAgent
	}

	agentIDs := make([]string, len(agents))
	for i, agent := range agents {
		agentIDs[i] = agent.ID
	}

	req := &ScoreRequest{
		RequiredSkills: requiredSkills,
	}

	scores, err := s.scoring.ScoreAgents(ctx, agentIDs, req)
	if err != nil {
		return nil, err
	}

	if len(scores) == 0 {
		return nil, ErrNoAvailableAgent
	}

	best, _ := s.pickBest(ctx, scores, "")
	return &best, nil
}

// StartWatchdog 启动Watchdog
func (s *Scheduler) StartWatchdog(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	go s.watchdogLoop(ctx)
}

// StopWatchdog 停止Watchdog
func (s *Scheduler) StopWatchdog() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	close(s.stopCh)
	s.running = false
}

func (s *Scheduler) watchdogLoop(ctx context.Context) {
	ticker := time.NewTicker(WatchdogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.checkTimeouts(ctx)
		}
	}
}

func (s *Scheduler) checkTimeouts(ctx context.Context) {
	// 检查超时的running任务
	// 实际实现需要从外部store获取running状态的任务
	// 这里简化处理，依赖registry清理离线Agent
	s.logger.Debug("Watchdog check triggered")
}

// RetryTask 重试任务
func (s *Scheduler) RetryTask(ctx context.Context, task *Task, maxRetry int) error {
	if task.RetryCount >= maxRetry {
		return ErrMaxRetries
	}

	task.RetryCount++
	task.Status = TaskStatusPending
	task.AssignedAgent = ""

	return nil
}

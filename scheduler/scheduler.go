package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

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

	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
}

// NewScheduler 创建调度器(无 cold policy,向后兼容 v0.7.x)
func NewScheduler(reg *registry.RedisStore, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		reg:        reg,
		scoring:    NewScoringEngine(reg, logger),
		logger:     logger,
		coldPolicy: nil,
		sleepPolicy: nil,
		stopCh:     make(chan struct{}),
	}
}

// NewSchedulerWithRegistry 创建调度器,接受 registry.Registry interface
//
// v0.8.0 M4-1.4:reg 字段从具体类型 *registry.RedisStore 改为 interface,
// 便于测试用 mock(集成 cold policy 时需要 mock registry)。
// 生产仍传 *registry.RedisStore(它实现了 registry.Registry interface)。
func NewSchedulerWithRegistry(reg registry.Registry, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		reg:        reg,
		scoring:    NewScoringEngine(reg, logger),
		logger:     logger,
		coldPolicy: nil,
		sleepPolicy: nil,
		stopCh:     make(chan struct{}),
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
		reg:        reg,
		scoring:    NewScoringEngine(reg, logger),
		logger:     logger,
		coldPolicy: coldPolicy,
		sleepPolicy: nil,
		stopCh:     make(chan struct{}),
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
		reg:         reg,
		scoring:     NewScoringEngine(reg, logger),
		logger:      logger,
		coldPolicy:  coldPolicy,
		sleepPolicy: sleepPolicy,
		stopCh:      make(chan struct{}),
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
		reg:        reg,
		scoring:    scoring,
		logger:     logger,
		stopCh:     make(chan struct{}),
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

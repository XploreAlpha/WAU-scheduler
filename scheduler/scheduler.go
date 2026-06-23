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

// Schedule 调度任务 - 选择最佳Agent
//
// v0.8.0 M4-1.4:如果设置了 coldPolicy,Schedule 会:
//   1. 调 ScoreAgents 拿 15 维打分
//   2. 调 coldPolicy.ShouldExplore(req.TaskID) 决定 explore vs warm
//   3. explore → SelectCold 从 cold 池选;warm → 取最高分
//   4. 无 cold 候选时,fallback warm 池
//
// 否则(nil policy)走原 15 维打分,完全向后兼容。
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

	// 4. 选择最佳Agent(15 维 + 可选 cold policy)
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

// pickBest applies cold policy (if set) to choose among scored candidates.
// Returns (chosen, coldRouted) where coldRouted=true means the choice
// came from the cold explore pool rather than the warm 15-dim ranking.
func (s *Scheduler) pickBest(ctx context.Context, scores []AgentScore, requestID string) (AgentScore, bool) {
	s.mu.RLock()
	policy := s.coldPolicy
	s.mu.RUnlock()

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

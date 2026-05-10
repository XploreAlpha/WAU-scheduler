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
	reg     *registry.RedisStore
	scoring *ScoringEngine
	logger  *slog.Logger

	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
}

// NewScheduler 创建调度器
func NewScheduler(reg *registry.RedisStore, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		reg:     reg,
		scoring: NewScoringEngine(reg, logger),
		logger:  logger,
		stopCh:  make(chan struct{}),
	}
}

// Schedule 调度任务 - 选择最佳Agent
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

	// 4. 选择最佳Agent
	best := scores[0]

	result := &ScheduleResult{
		Task:          req.Task,
		AgentID:       best.AgentID,
		Score:         best.TotalScore,
		DispatchedAt:  time.Now(),
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
	)

	return result, nil
}

// ScheduleSimple 简化调度 - 输入所需技能列表，返回最佳Agent
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

	return &scores[0], nil
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

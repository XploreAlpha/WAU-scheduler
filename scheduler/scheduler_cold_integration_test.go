package scheduler

import (
	"context"
	"fmt"
	"testing"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// =====================================================================
// Schedule() 集成 cold policy 测试 (v0.8.0 M4-1.4)
//
// 覆盖:
//   1. NewSchedulerWithRegistry 创建无 policy scheduler(向后兼容)
//   2. NewSchedulerWithColdPolicy 创建有 policy scheduler
//   3. Schedule() 在 cold policy nil 时走原 15 维打分
//   4. Schedule() 在 cold policy explore 时走 cold pool
//   5. Schedule() 在 cold policy 无 cold 候选时 fallback warm
//   6. ScheduleSimple() 同样应用 cold policy
//   7. SetColdPolicy 动态切换 policy
// =====================================================================

// makeColdIntegrationRig creates a scheduler with 3 agents: 1 warm + 2 cold.
// Returns the scheduler, the trust engine (for test setup), and the mock registry.
func makeColdIntegrationRig(t *testing.T, withColdPolicy bool) (*Scheduler, *engine.MemoryEngine, *integrationMockRegistry) {
	t.Helper()
	logger := testLoggerCold()
	trustEng := engine.NewMemoryEngine()
	ctx := context.Background()

	// Setup registry: Whis (warm), Fox (cold), Benny (cold)
	whis := &registry.AgentCard{ID: "Whis", Name: "Whis", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"}
	fox := &registry.AgentCard{ID: "Fox", Name: "Fox", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"}
	benny := &registry.AgentCard{ID: "Benny", Name: "Benny", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"}
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Whis":  whis,
		"Fox":   fox,
		"Benny": benny,
	}}

	// Mark Whis as warm via trust engine
	_ = trustEng.RecordSuccess(ctx, "Whis", 0.5)
	_ = trustEng.RecordSuccess(ctx, "Whis", 0.5)

	ds := NewWauTrustDataSource(
		NewMemoryDataSource(mockReg, WithKernelVersion("0.7.0")),
		trustEng,
	)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, logger)

	var s *Scheduler
	if withColdPolicy {
		policy := NewColdRoutingPolicy(0.10, 10, 0.5, logger)
		s = NewSchedulerWithColdPolicy(mockReg, logger, policy)
	} else {
		s = NewSchedulerWithRegistry(mockReg, logger)
	}
	// Replace the scoring engine with our WauTrust-aware one
	// (NewSchedulerWithRegistry creates a new scoring engine internally
	//  without DataSource — we want WauTrust to be plumbed for IsCold)
	s.scoring = scoring
	return s, trustEng, mockReg
}

// TestScheduler_NilColdPolicy_BackwardCompat: 无 policy → 15 维打分,跟 v0.7.x 一致
// Whis 是唯一的 warm agent(其他 2 个 cold,默认 trust 0.5)
// 因为 TrustScore 维度都是 0.5,SkillMatch 都是 1.0 → 平分
// 此时选哪个? 取决于 sort 稳定性(15 维总 score 一样)
// 我们只验证:不 panic + 返回一个有效 agent
func TestScheduler_NilColdPolicy_BackwardCompat(t *testing.T) {
	s, _, _ := makeColdIntegrationRig(t, false)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		req := &ScheduleRequest{
			Task: &Task{
				TaskID: fmt.Sprintf("task-%d", i),
				Status: TaskStatusPending,
			},
			RequiredSkills: []string{"translate"},
		}
		result, err := s.Schedule(ctx, req)
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		if result.AgentID == "" {
			t.Error("AgentID should be set")
		}
		if result.Task == nil || result.Task.Status != TaskStatusDispatched {
			t.Errorf("Task status should be dispatched, got %v", result.Task)
		}
		if result.Task.AssignedAgent != result.AgentID {
			t.Errorf("Task.AssignedAgent %q != AgentID %q", result.Task.AssignedAgent, result.AgentID)
		}
	}
}

// TestScheduler_WithColdPolicy_ExploreMode: 有 policy + explore 决策 → 选 cold agent
// 用 100% explore budget 强制走 explore
func TestScheduler_WithColdPolicy_ExploreMode(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerCold()

	s, _, _ := makeColdIntegrationRig(t, true) // 10% explore by default
	// Force 100% explore by replacing policy
	s.SetColdPolicy(NewColdRoutingPolicy(1.0, 10, 0.5, logger))

	// Force explore by using task IDs that hash to < 1.0 (any taskID)
	// With budget=1.0, ShouldExplore always returns true
	coldCount := 0
	for i := 0; i < 20; i++ {
		req := &ScheduleRequest{
			Task: &Task{
				TaskID: fmt.Sprintf("task-%d", i),
				Status: TaskStatusPending,
			},
			RequiredSkills: []string{"translate"},
		}
		result, err := s.Schedule(ctx, req)
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		// Cold agents: Fox, Benny
		if result.AgentID == "Fox" || result.AgentID == "Benny" {
			coldCount++
		}
	}
	// 100% explore budget → all 20 should pick from cold pool
	if coldCount != 20 {
		t.Errorf("with 100%% explore budget, expected 20 cold selections, got %d", coldCount)
	}
}

// TestScheduler_WithColdPolicy_WarmFallback: 有 policy 但 cold 候选都不满足 skill floor
// → 选 warm agent(Whis)
func TestScheduler_WithColdPolicy_WarmFallback(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerCold()

	s, trustEng, _ := makeColdIntegrationRig(t, true)
	// Override cold pool: add a cold agent with low skill match
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Whis":      {ID: "Whis", Name: "Whis", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"},
		"Cold-LowS": {ID: "Cold-LowS", Name: "Cold-LowS", Skills: []string{"data-analysis"}, Universe: "us-west-1", Version: "0.7.0"},
	}}
	s.reg = mockReg
	_ = s.scoring // scoring is already configured
	// Recompute scoring engine with the new mock registry
	ds := NewWauTrustDataSource(
		NewMemoryDataSource(mockReg, WithKernelVersion("0.7.0")),
		trustEng,
	)
	s.scoring = NewScoringEngineWithDataSource(mockReg, ds, logger)

	// Set policy to 100% explore, but skill floor 0.9 → Cold-LowS (skill=0.0) fails
	policy := NewColdRoutingPolicy(1.0, 10, 0.9, logger)
	s.SetColdPolicy(policy)

	req := &ScheduleRequest{
		Task: &Task{
			TaskID: "fallback-test",
			Status: TaskStatusPending,
		},
		RequiredSkills: []string{"translate"},
	}
	result, err := s.Schedule(ctx, req)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	// Cold-LowS can't meet skill floor 0.9 for "translate" → fallback to warm
	if result.AgentID != "Whis" {
		t.Errorf("expected fallback to warm Whis, got %q", result.AgentID)
	}
}

// TestScheduler_ColdPolicy_Distribution: 50% explore budget, 20 个 task
// → 期望 ~10 个 cold 选中(0.5 * 20 = 10,允许 6-14 范围)
func TestScheduler_ColdPolicy_Distribution(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerCold()

	s, _, _ := makeColdIntegrationRig(t, true)
	// 50% explore budget for measurable distribution
	s.SetColdPolicy(NewColdRoutingPolicy(0.5, 10, 0.5, logger))

	coldCount := 0
	const N = 100
	for i := 0; i < N; i++ {
		req := &ScheduleRequest{
			Task: &Task{
				TaskID: fmt.Sprintf("task-%05d", i),
				Status: TaskStatusPending,
			},
			RequiredSkills: []string{"translate"},
		}
		result, err := s.Schedule(ctx, req)
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		if result.AgentID == "Fox" || result.AgentID == "Benny" {
			coldCount++
		}
	}
	// 50% budget × 100 = ~50, allow 30-70
	if coldCount < 30 || coldCount > 70 {
		t.Errorf("expected ~50 cold selections, got %d (range 30-70)", coldCount)
	}
	t.Logf("cold selection rate: %d/%d = %f (budget=0.5)", coldCount, N, float64(coldCount)/float64(N))
}

// TestScheduler_ScheduleSimple_RespectsPolicy: ScheduleSimple 同样应用 cold policy
func TestScheduler_ScheduleSimple_RespectsPolicy(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerCold()

	s, _, _ := makeColdIntegrationRig(t, true)
	// 100% explore
	s.SetColdPolicy(NewColdRoutingPolicy(1.0, 10, 0.5, logger))

	coldCount := 0
	for i := 0; i < 10; i++ {
		score, err := s.ScheduleSimple(ctx, []string{"translate"})
		if err != nil {
			t.Fatalf("ScheduleSimple: %v", err)
		}
		if score.AgentID == "Fox" || score.AgentID == "Benny" {
			coldCount++
		}
	}
	// 100% explore + ScheduleSimple uses "" requestID (LCG fallback)
	// LCG should distribute explore decisions across all 10 calls
	if coldCount < 5 {
		t.Errorf("with 100%% explore, expected at least 5 cold selections, got %d", coldCount)
	}
}

// TestScheduler_SetColdPolicy_Dynamic: 动态切换 policy
// 1. 无 policy → 走 warm
// 2. 设 100% explore policy → 走 cold
// 3. 设回 nil policy → 走 warm
func TestScheduler_SetColdPolicy_Dynamic(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerCold()

	s, _, _ := makeColdIntegrationRig(t, false) // start without policy

	req := &ScheduleRequest{
		Task:           &Task{TaskID: "test-dynamic", Status: TaskStatusPending},
		RequiredSkills: []string{"translate"},
	}

	// Step 1: no policy → no panic, returns valid agent
	r1, err := s.Schedule(ctx, req)
	if err != nil {
		t.Fatalf("Schedule (no policy): %v", err)
	}
	if r1.AgentID == "" {
		t.Error("Step 1: should return a valid agent")
	}

	// Step 2: set 100% explore policy → cold pool
	s.SetColdPolicy(NewColdRoutingPolicy(1.0, 10, 0.5, logger))
	coldCount := 0
	for i := 0; i < 10; i++ {
		r, err := s.Schedule(ctx, &ScheduleRequest{
			Task:           &Task{TaskID: fmt.Sprintf("test-d-%d", i), Status: TaskStatusPending},
			RequiredSkills: []string{"translate"},
		})
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		if r.AgentID == "Fox" || r.AgentID == "Benny" {
			coldCount++
		}
	}
	if coldCount != 10 {
		t.Errorf("Step 2: with 100%% explore, expected 10 cold, got %d", coldCount)
	}

	// Step 3: set policy to nil → warm pool (back to legacy)
	s.SetColdPolicy(nil)
	_, err = s.Schedule(ctx, req)
	if err != nil {
		t.Fatalf("Schedule (nil policy): %v", err)
	}
	// No assertion on which agent (depends on 15-dim tie-breaking) —
	// just verify it runs without panic and returns valid result
}

// TestScheduler_NoAgents_ErrNoAvailableAgent: 0 agents → 返 ErrNoAvailableAgent
func TestScheduler_NoAgents_ErrNoAvailableAgent(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerCold()
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{}}
	s := NewSchedulerWithColdPolicy(mockReg, logger, NewColdRoutingPolicy(1.0, 10, 0.5, logger))

	req := &ScheduleRequest{
		Task:           &Task{TaskID: "no-agents", Status: TaskStatusPending},
		RequiredSkills: []string{"translate"},
	}
	_, err := s.Schedule(ctx, req)
	if err != ErrNoAvailableAgent {
		t.Errorf("expected ErrNoAvailableAgent, got %v", err)
	}
}

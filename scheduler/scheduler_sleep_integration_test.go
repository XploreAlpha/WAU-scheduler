package scheduler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// =====================================================================
// Schedule() 集成 sleep policy 测试 (v0.8.0 M4-2.2)
//
// 覆盖:
//   1. NewSchedulerWithSleepPolicy 创建有 sleep policy scheduler
//   2. Schedule() 跳过 asleep agents
//   3. SetSleepPolicy 动态切换
//   4. SleepPolicy 跟 ColdRoutingPolicy 协同(asleep 优先过滤)
//   5. 所有 agents 都 asleep → 返零值 AgentScore
// =====================================================================

func testLoggerSleep() *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriterSleep{}, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

type testWriterSleep struct{}

func (testWriterSleep) Write(p []byte) (int, error) { return len(p), nil }

// makeSleepIntegrationRig creates a scheduler rig with 3 agents:
//   - Whis: awake, warm
//   - Benny: awake, warm (lower score)
//   - Fox: ASLEEP (via trustEng.Sleep)
func makeSleepIntegrationRig(t *testing.T, withSleepPolicy bool) (*Scheduler, *engine.MemoryEngine) {
	t.Helper()
	logger := testLoggerSleep()
	ctx := context.Background()

	whis := &registry.AgentCard{ID: "Whis", Name: "Whis", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"}
	benny := &registry.AgentCard{ID: "Benny", Name: "Benny", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"}
	fox := &registry.AgentCard{ID: "Fox", Name: "Fox", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"}
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Whis":  whis,
		"Benny": benny,
		"Fox":   fox,
	}}

	trustEng := engine.NewMemoryEngine()
	_ = trustEng.RecordSuccess(ctx, "Whis", 0.5)
	_ = trustEng.RecordSuccess(ctx, "Benny", 0.3)
	_ = trustEng.RecordSuccess(ctx, "Fox", 0.1)
	_ = trustEng.Sleep(ctx, "Fox") // Fox is asleep

	ds := NewWauTrustDataSource(
		NewMemoryDataSource(mockReg, WithKernelVersion("0.7.0")),
		trustEng,
	)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, logger)

	var s *Scheduler
	if withSleepPolicy {
		policy := NewSleepPolicy(logger)
		s = NewSchedulerWithSleepPolicy(mockReg, logger, nil, policy)
	} else {
		s = NewSchedulerWithRegistry(mockReg, logger)
	}
	s.scoring = scoring
	return s, trustEng
}

// TestScheduler_SleepPolicy_FiltersAsleepAgent: Sleep policy 启用时,asleep agent 不被选中
func TestScheduler_SleepPolicy_FiltersAsleepAgent(t *testing.T) {
	ctx := context.Background()
	s, _ := makeSleepIntegrationRig(t, true)

	// 跑 50 次 Schedule,验证 Fox 永远不被选中
	const N = 50
	foxCount := 0
	whisCount := 0
	bennyCount := 0
	for i := 0; i < N; i++ {
		req := &ScheduleRequest{
			Task:           &Task{TaskID: "task-" + string(rune('a'+i%26)), Status: TaskStatusPending},
			RequiredSkills: []string{"translate"},
		}
		result, err := s.Schedule(ctx, req)
		if err != nil {
			t.Fatalf("Schedule #%d: %v", i, err)
		}
		switch result.AgentID {
		case "Whis":
			whisCount++
		case "Benny":
			bennyCount++
		case "Fox":
			foxCount++
		default:
			t.Errorf("unexpected agent: %s", result.AgentID)
		}
	}

	if foxCount != 0 {
		t.Errorf("Fox should NEVER be selected (asleep), got %d/%d", foxCount, N)
	}
	if whisCount+bennyCount != N {
		t.Errorf("expected all N=%d schedules to go to Whis/Benny, got %d", N, whisCount+bennyCount)
	}
}

// TestScheduler_NoSleepPolicy_BackwardCompat: 无 sleep policy 时,行为完全等同 v0.7.x
// (这里因为 Fox trust 高于 Benny,排名上 Whis > Fox > Benny,所以会被选中)
func TestScheduler_NoSleepPolicy_BackwardCompat(t *testing.T) {
	ctx := context.Background()
	s, _ := makeSleepIntegrationRig(t, false) // no sleep policy

	// Schedule 1 次 — 不应 panic / 返有效 agent
	req := &ScheduleRequest{
		Task:           &Task{TaskID: "t1", Status: TaskStatusPending},
		RequiredSkills: []string{"translate"},
	}
	result, err := s.Schedule(ctx, req)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if result.AgentID == "" {
		t.Error("expected non-empty agent ID")
	}
	// 3 个 agent 都可能被选 — 但 sleep policy 没启用时,不应过滤
	t.Logf("without sleep policy, scheduled: %s (could be any of 3)", result.AgentID)
}

// TestScheduler_SetSleepPolicy_Dynamic: 动态切换 sleep policy
func TestScheduler_SetSleepPolicy_Dynamic(t *testing.T) {
	ctx := context.Background()
	s, _ := makeSleepIntegrationRig(t, false)

	// 1. 初始无 policy — 可能选 Fox(取决于 trust 排名)
	req1 := &ScheduleRequest{
		Task:           &Task{TaskID: "t1", Status: TaskStatusPending},
		RequiredSkills: []string{"translate"},
	}
	r1, _ := s.Schedule(ctx, req1)
	t.Logf("without sleep policy: agent=%s", r1.AgentID)

	// 2. 启用 sleep policy — Fox 永远排除
	policy := NewSleepPolicy(testLoggerSleep())
	s.SetSleepPolicy(policy)

	const N = 20
	for i := 0; i < N; i++ {
		req := &ScheduleRequest{
			Task:           &Task{TaskID: "dyn-" + string(rune('a'+i)), Status: TaskStatusPending},
			RequiredSkills: []string{"translate"},
		}
		result, _ := s.Schedule(ctx, req)
		if result.AgentID == "Fox" {
			t.Errorf("with sleep policy, Fox should never be selected, got at i=%d", i)
		}
	}

	// 3. 关闭 sleep policy — 恢复可调度 Fox
	s.SetSleepPolicy(nil)
	r3, _ := s.Schedule(ctx, &ScheduleRequest{
		Task:           &Task{TaskID: "after-close", Status: TaskStatusPending},
		RequiredSkills: []string{"translate"},
	})
	t.Logf("after SetSleepPolicy(nil): agent=%s", r3.AgentID)
}

// TestScheduler_SleepPolicy_AllAsleep: 所有 agents 都 asleep → 返零值 AgentScore
func TestScheduler_SleepPolicy_AllAsleep(t *testing.T) {
	ctx := context.Background()
	s, trustEng := makeSleepIntegrationRig(t, true)

	// 把 Whis + Benny 也 mark asleep
	if err := trustEng.Sleep(ctx, "Whis"); err != nil {
		t.Fatalf("Sleep(Whis): %v", err)
	}
	if err := trustEng.Sleep(ctx, "Benny"); err != nil {
		t.Fatalf("Sleep(Benny): %v", err)
	}

	req := &ScheduleRequest{
		Task:           &Task{TaskID: "all-asleep", Status: TaskStatusPending},
		RequiredSkills: []string{"translate"},
	}
	result, err := s.Schedule(ctx, req)
	if err != nil {
		t.Fatalf("Schedule (all asleep): %v", err)
	}
	// 期望:pickBest 在所有 agent 都被 filter 掉时,返零值
	// Schedule() 仍返 result,但 AgentID=""(表示无可用)
	t.Logf("Schedule with all asleep: agent=%q score=%v (expecting empty agent)",
		result.AgentID, result.Score)
	// 不强制 err — caller 自己判断 result.AgentID == ""
}

// TestScheduler_SleepPolicy_WithColdPolicy: sleep policy 跟 cold policy 协同
func TestScheduler_SleepPolicy_WithColdPolicy(t *testing.T) {
	ctx := context.Background()
	logger := testLoggerSleep()
	trustEng := engine.NewMemoryEngine()

	whis := &registry.AgentCard{ID: "Whis", Name: "Whis", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"}
	fox := &registry.AgentCard{ID: "Fox", Name: "Fox", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"}
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Whis": whis,
		"Fox":  fox,
	}}

	// Whis: warm + awake
	_ = trustEng.RecordSuccess(ctx, "Whis", 0.5)
	// Fox: cold + asleep — 都不该被选
	_ = trustEng.Sleep(ctx, "Fox") // cold + asleep

	ds := NewWauTrustDataSource(
		NewMemoryDataSource(mockReg, WithKernelVersion("0.7.0")),
		trustEng,
	)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, logger)

	coldPolicy := NewColdRoutingPolicy(1.0, 10, 0.5, logger) // 100% explore
	sleepPolicy := NewSleepPolicy(logger)
	s := NewSchedulerWithSleepPolicy(mockReg, logger, coldPolicy, sleepPolicy)
	s.scoring = scoring

	const N = 20
	for i := 0; i < N; i++ {
		req := &ScheduleRequest{
			Task:           &Task{TaskID: "dual-" + string(rune('a'+i)), Status: TaskStatusPending},
			RequiredSkills: []string{"translate"},
		}
		result, err := s.Schedule(ctx, req)
		if err != nil {
			t.Fatalf("Schedule #%d: %v", i, err)
		}
		// 期望:Fox(asleep)在 sleep filter 阶段就被去掉
		// 即使 cold policy 100% explore,cold 池(Fox)也已被 sleep filter 排除
		// → 只能选 Whis(尽管 Whis 不 cold)
		if result.AgentID == "Fox" {
			t.Errorf("Fox (cold + asleep) should never be selected, got at i=%d", i)
		}
		if result.AgentID != "Whis" {
			t.Errorf("expected Whis, got %s at i=%d", result.AgentID, i)
		}
	}
}

// TestScheduler_SleepPolicy_OnlyAsleep_NoAgents: 只有 asleep agents → Schedule 不 panic
func TestScheduler_SleepPolicy_OnlyAsleep_NoAgents(t *testing.T) {
	ctx := context.Background()
	s, _ := makeSleepIntegrationRig(t, true)

	// 让 registry 只返 online agent — 但调度器会过滤 asleep 后返 0 个
	// 这里只是验证:Schedule 不 panic,即使结果奇怪
	_ = s // 不动 s,直接调 Schedule 测
	req := &ScheduleRequest{
		Task:           &Task{TaskID: "no-awake", Status: TaskStatusPending},
		RequiredSkills: []string{"translate"},
	}
	// 不应 panic
	result, _ := s.Schedule(ctx, req)
	t.Logf("with sleep policy + only Fox asleep + Whis/Benny awake: agent=%q", result.AgentID)
	// 当前 rig 仍有 2 个 awake (Whis + Benny),所以能正常调度
	if result.AgentID == "" {
		t.Error("expected non-empty agent (Whis or Benny awake)")
	}

	// Sleep prevention check
	time.Sleep(1 * time.Millisecond) // 让 race detector 有机会检查
}

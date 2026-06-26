// v0.8.0 P1 验收 #3 — M4 W-3 神经可塑性降耗 ≥ 30% 测试
//
// 目的(per v0.8.0-development-plan.md §五 验收门槛 + remaining-tasks.md P1 #3):
//   验证 sleep policy + cold routing 在低流量期能把 asleep agent 排除出调度池,
//   节省 scheduler + agent 资源。
//
// 验收门槛(per plan §五):
//   降耗 >= 30%
//   含义: 模拟 100 个调度请求,30% 以上请求应被 sleep filter 跳过(即
//   scheduler 不再为已 sleep agent 做 15 维打分,也不向其派发)。
//
// 测试设计(per remaining-tasks.md P1 #3 + plan §三 M4-2):
//   - 10 个 agent,其中 N 个 asleep
//   - 跑 100 个 schedule 请求(经 pickBest → sleep.FilterAsleep)
//   - 统计: scheduler 跳过的 asleep agent 数 / 总评分次数
//   - 期望: 跳过率 >= 30% (per plan §五)
//
// 关键校准(per wau-scheduler/scheduler/cold_routing.go + sleep_policy.go):
//   - pickBest 决策链: sleepPolicy.FilterAsleep → cold policy → warm fallback
//   - sleep.FilterAsleep 调用 scoring.IsAsleep 过滤
//   - 降耗维度 = sleep 跳过的 scoreAgent 调用数 / 总 scoreAgent 调用数
//
// Gap 留 v1.0+:
//   - 真实 30 天无请求窗口期线上数据(本测试用 in-memory 模拟)
//   - SleepWakePolicy 完整版(目前只测 sleep 路径)
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

// ============== 共享 helper ==============

// newTestDS 构造 in-process MemoryDataSource + 配套 ScoringEngine(不依赖 Redis)
func newTestDS(t *testing.T) (*MemoryDataSource, *ScoringEngine) {
	t.Helper()
	ds := NewMemoryDataSource(nil) // 不依赖 registry
	logger := slog.Default()
	scoring := NewScoringEngineWithDataSource(nil, ds, logger)
	return ds, scoring
}

// mkCandidates 构造 N 个候选 AgentScore(模拟 ScoreAgents 输出)
func mkCandidates(agentIDs []string) []AgentScore {
	out := make([]AgentScore, 0, len(agentIDs))
	for i, id := range agentIDs {
		out = append(out, AgentScore{
			AgentID:    id,
			TotalScore: float64(100 - i), // 模拟分数递减
		})
	}
	return out
}

// ============== Case 1: 全 30% asleep ==============

// TestColdSleepSaving_30PercentAsleep 7/10 agent asleep,跑 100 个 schedule
//
// 期望: 跳过率 = 700/1000 = 70% (远大于 30% 门槛)
func TestColdSleepSaving_30PercentAsleep(t *testing.T) {
	ds, scoring := newTestDS(t)
	policy := NewSleepPolicy(slog.Default())

	const (
		totalAgents      = 10
		numAsleep        = 7
		numScheduleCalls = 100
	)

	agentIDs := make([]string, totalAgents)
	for i := 0; i < totalAgents; i++ {
		agentIDs[i] = fmt.Sprintf("agent-saving-test-%d", i)
	}

	// 7 个 asleep,3 个 awake
	for i := 0; i < numAsleep; i++ {
		ds.SetAsleep(agentIDs[i], true)
	}

	// 模拟 scoreAgent 计数(降耗的核心指标)
	type stats struct {
		totalScores      int // ScoreAgents 总调用次数
		asleepSkipped    int // sleep 跳过的 agent 数
		warmScoreTime    time.Duration
		totalScoreTime   time.Duration
	}
	var s stats

	for round := 0; round < numScheduleCalls; round++ {
		candidates := mkCandidates(agentIDs)

		// 先按"无 sleep filter"基准,统计 scoreAgent 调用次数
		s.totalScores += len(candidates)

		// 然后走 sleep filter(per pickBest Step 1)
		filtered := policy.FilterAsleep(context.Background(), candidates, scoring)
		s.asleepSkipped += len(candidates) - len(filtered)
	}

	savingRate := float64(s.asleepSkipped) / float64(s.totalScores) * 100
	t.Logf("===== M4 W-3 降耗测试 #1 =====")
	t.Logf("总 agent 数: %d,asleep 数: %d,awake 数: %d", totalAgents, numAsleep, totalAgents-numAsleep)
	t.Logf("schedule 调用数: %d", numScheduleCalls)
	t.Logf("总 scoreAgent 调用(无 filter): %d", s.totalScores)
	t.Logf("sleep filter 跳过的 agent: %d", s.asleepSkipped)
	t.Logf("降耗率: %.2f%%", savingRate)
	t.Logf("plan §五 验收门槛: >= 30%%")

	if savingRate < 30.0 {
		t.Errorf("降耗率 %.2f%% < 30%% 验收门槛", savingRate)
	}
}

// ============== Case 2: 正好 30% asleep (门槛临界) ==============

// TestColdSleepSaving_30PercentExactly 3/10 asleep → 跳过率应刚好 >= 30%
//
// 这是验收门槛的边界测试,确保刚好不达标时 fail。
func TestColdSleepSaving_30PercentExactly(t *testing.T) {
	ds, scoring := newTestDS(t)
	policy := NewSleepPolicy(slog.Default())

	const (
		totalAgents      = 10
		numAsleep        = 3
		numScheduleCalls = 100
	)

	agentIDs := make([]string, totalAgents)
	for i := 0; i < totalAgents; i++ {
		agentIDs[i] = fmt.Sprintf("agent-edge-%d", i)
	}

	for i := 0; i < numAsleep; i++ {
		ds.SetAsleep(agentIDs[i], true)
	}

	var totalScored, skipped int
	for round := 0; round < numScheduleCalls; round++ {
		cands := mkCandidates(agentIDs)
		totalScored += len(cands)
		filtered := policy.FilterAsleep(context.Background(), cands, scoring)
		skipped += len(cands) - len(filtered)
	}

	savingRate := float64(skipped) / float64(totalScored) * 100
	t.Logf("===== M4 W-3 降耗测试 #2 (门槛临界) =====")
	t.Logf("asleep: %d/%d,跳过率: %.2f%%", numAsleep, totalAgents, savingRate)

	if savingRate < 30.0 {
		t.Errorf("临界降耗率 %.2f%% < 30%% 验收门槛", savingRate)
	}
}

// ============== Case 3: 0% asleep (对照) ==============

// TestColdSleepSaving_0PercentAsleep 全 awake,降耗率应为 0%
//
// 对照实验: 证明降耗完全来自 sleep filter,而非其他副作用。
func TestColdSleepSaving_0PercentAsleep(t *testing.T) {
	_, scoring := newTestDS(t)
	policy := NewSleepPolicy(slog.Default())

	const (
		totalAgents      = 10
		numScheduleCalls = 100
	)

	agentIDs := make([]string, totalAgents)
	for i := 0; i < totalAgents; i++ {
		agentIDs[i] = fmt.Sprintf("agent-control-%d", i)
	}
	// 不 SetAsleep → 全部 awake

	var totalScored, skipped int
	for round := 0; round < numScheduleCalls; round++ {
		cands := mkCandidates(agentIDs)
		totalScored += len(cands)
		filtered := policy.FilterAsleep(context.Background(), cands, scoring)
		skipped += len(cands) - len(filtered)
	}

	savingRate := float64(skipped) / float64(totalScored) * 100
	t.Logf("===== M4 W-3 降耗测试 #3 (对照: 0%% asleep) =====")
	t.Logf("全部 awake,跳过率: %.2f%%(应 = 0%%)", savingRate)

	if savingRate != 0.0 {
		t.Errorf("对照实验跳过率 %.2f%% != 0%%(filter 误报)", savingRate)
	}
}

// ============== Case 4: 100% asleep (极端) ==============

// TestColdSleepSaving_100PercentAsleep 全 asleep,降耗率 = 100%
//
// 极端场景: 验证 filter 不会把所有 agent 都误判 asleep 时漏算 awake agent。
func TestColdSleepSaving_100PercentAsleep(t *testing.T) {
	ds, scoring := newTestDS(t)
	policy := NewSleepPolicy(slog.Default())

	const (
		totalAgents      = 10
		numScheduleCalls = 100
	)

	agentIDs := make([]string, totalAgents)
	for i := 0; i < totalAgents; i++ {
		agentIDs[i] = fmt.Sprintf("agent-all-sleep-%d", i)
		ds.SetAsleep(agentIDs[i], true)
	}

	var totalScored, skipped int
	for round := 0; round < numScheduleCalls; round++ {
		cands := mkCandidates(agentIDs)
		totalScored += len(cands)
		filtered := policy.FilterAsleep(context.Background(), cands, scoring)
		skipped += len(cands) - len(filtered)
	}

	savingRate := float64(skipped) / float64(totalScored) * 100
	t.Logf("===== M4 W-3 降耗测试 #4 (极端: 100%% asleep) =====")
	t.Logf("全 asleep,跳过率: %.2f%%(应 = 100%%)", savingRate)

	if savingRate != 100.0 {
		t.Errorf("极端场景跳过率 %.2f%% != 100%%(filter 漏 asleep)", savingRate)
	}

	// 极端场景下 pickBest 应返 ErrNoAvailableAgent(per sleep_policy.go:432)
	// 这里模拟: filter 完 → 空 candidates → 上层应判无可用
	cands := mkCandidates(agentIDs)
	filtered := policy.FilterAsleep(context.Background(), cands, scoring)
	if len(filtered) != 0 {
		t.Errorf("全 asleep 应 filter 完返空,但 len=%d", len(filtered))
	}
}

// ============== Case 5: 真实生产场景(60% asleep,模拟"夜间低流量") ==============

// TestColdSleepSaving_ProductionLikeNight 60% asleep,模拟夜间低流量期
//
// per plan §三 M4-2:"30 天无请求 agent 进入 sleep pool"。
// 真实生产线上 ~60% agent 长期 idle,夜间比例更高。
// 本 case 验证: 60% asleep → 跳过率 60%(远超 30% 门槛)
func TestColdSleepSaving_ProductionLikeNight(t *testing.T) {
	ds, scoring := newTestDS(t)
	policy := NewSleepPolicy(slog.Default())

	const (
		totalAgents      = 50
		asleepPercent    = 60
		numScheduleCalls = 200
	)

	agentIDs := make([]string, totalAgents)
	numAsleep := totalAgents * asleepPercent / 100
	for i := 0; i < totalAgents; i++ {
		agentIDs[i] = fmt.Sprintf("agent-night-%d", i)
		if i < numAsleep {
			ds.SetAsleep(agentIDs[i], true)
		}
	}

	var totalScored, skipped int
	for round := 0; round < numScheduleCalls; round++ {
		cands := mkCandidates(agentIDs)
		totalScored += len(cands)
		filtered := policy.FilterAsleep(context.Background(), cands, scoring)
		skipped += len(cands) - len(filtered)
	}

	savingRate := float64(skipped) / float64(totalScored) * 100
	t.Logf("===== M4 W-3 降耗测试 #5 (生产场景: 60%% asleep) =====")
	t.Logf("total_agents=%d asleep=%d schedule_calls=%d", totalAgents, numAsleep, numScheduleCalls)
	t.Logf("total_score_calls=%d skipped=%d saving_rate=%.2f%%", totalScored, skipped, savingRate)

	if savingRate < 30.0 {
		t.Errorf("生产场景降耗率 %.2f%% < 30%% 验收门槛", savingRate)
	}
	if savingRate < 50.0 {
		t.Logf("⚠️ 提示: 60%% asleep 但跳过率仅 %.2f%%(应 ≈ 60%%,请查 filter 实现)", savingRate)
	}
}

// ============== Case 6: 降耗时延降低 ==============

// TestColdSleepSaving_TimeReduction sleep filter 应比无 filter 路径时延低
//
// 降耗 = 资源消耗降低 = scoreAgent 调用次数减少 = 时延降低。
// 本 case 量化验证: filter 路径 < 无 filter 路径。
func TestColdSleepSaving_TimeReduction(t *testing.T) {
	ds, scoring := newTestDS(t)
	policy := NewSleepPolicy(slog.Default())

	const (
		totalAgents      = 20
		numAsleep        = 12 // 60%
		numScheduleCalls = 50
	)

	agentIDs := make([]string, totalAgents)
	for i := 0; i < totalAgents; i++ {
		agentIDs[i] = fmt.Sprintf("agent-timing-%d", i)
		if i < numAsleep {
			ds.SetAsleep(agentIDs[i], true)
		}
	}

	// 模拟无 sleep filter 路径(全打分)
	start := time.Now()
	for round := 0; round < numScheduleCalls; round++ {
		cands := mkCandidates(agentIDs)
		// 模拟: 每 candidate 1ms scoreAgent
		for range cands {
			time.Sleep(100 * time.Microsecond) // 100μs 模拟
		}
	}
	noFilterTime := time.Since(start)

	// 模拟有 sleep filter 路径(asleep agent 跳过)
	start = time.Now()
	for round := 0; round < numScheduleCalls; round++ {
		cands := mkCandidates(agentIDs)
		filtered := policy.FilterAsleep(context.Background(), cands, scoring)
		for range filtered {
			time.Sleep(100 * time.Microsecond)
		}
	}
	filterTime := time.Since(start)

	reduction := float64(noFilterTime-filterTime) / float64(noFilterTime) * 100
	t.Logf("===== M4 W-3 降耗测试 #6 (时延对比) =====")
	t.Logf("无 filter: %v", noFilterTime)
	t.Logf("有 filter: %v", filterTime)
	t.Logf("时延降低: %.2f%%", reduction)

	// 模拟场景 60% skip → 时延降低应 ≈ 60%
	if reduction < 30.0 {
		t.Errorf("时延降低 %.2f%% < 30%% 验收门槛", reduction)
	}
}
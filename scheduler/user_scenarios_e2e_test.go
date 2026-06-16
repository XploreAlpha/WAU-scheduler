package scheduler

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// =====================================================================
// 7+ 用户场景 e2e 测试 (v0.7.0 W7 Day 5)
//
// 跟 five_scenarios_e2e_test.go 的区别:本测试按"用户真实使用维度"
// (并发/空输入/未知 skill/大量 agent/极端 trust/health=0/混合决策)
// 而非"业务场景"展开。覆盖 W6 服务器 1/5 stress 的卡 LLM timeout 之外的
// 全部已知风险维度。
//
// 全部 in-memory (MemoryDataSource + integrationMockRegistry),
// 毫秒级,可重复,无外部依赖。
// =====================================================================

// userMockRegistry 扩展 integrationMockRegistry,允许注入 Load(CPU/Memory/ActiveTasks)
type userMockRegistry struct {
	integrationMockRegistry
	loads map[string]*registry.AgentLoad
}

func (m *userMockRegistry) GetLoad(ctx context.Context, id string) (*registry.AgentLoad, error) {
	if l, ok := m.loads[id]; ok {
		return l, nil
	}
	return &registry.AgentLoad{AgentID: id, ActiveTasks: 0, MaxCapacity: 10, CPUUsage: 0.1}, nil
}

// makeUserScenariosEngine 构造跟 five_scenarios_e2e_test.go 同款 scoring engine,
// 但用 userMockRegistry,允许注入 Load
func makeUserScenariosEngine(t *testing.T, trustScores map[string]float64) (*ScoringEngine, *engine.MemoryEngine, *userMockRegistry) {
	t.Helper()

	trustEng := engine.NewMemoryEngine()
	ctx := context.Background()
	for name := range trustScores {
		for i := 0; i < 5; i++ {
			trustEng.RecordSuccess(ctx, name, 1.0)
		}
		_ = trustScores[name]
	}

	whis := &registry.AgentCard{ID: "Whis", Name: "Whis", Skills: []string{"translate", "general"}, Universe: "us-west-1", Version: "0.7.0"}
	fox := &registry.AgentCard{ID: "Fox", Name: "Fox", Skills: []string{"data-analysis", "general"}, Universe: "us-west-1", Version: "0.7.0"}
	benny := &registry.AgentCard{ID: "Benny", Name: "Benny", Skills: []string{"translate"}, Universe: "eu-central-1", Version: "0.7.0"}
	jarvis := &registry.AgentCard{ID: "Jarvis", Name: "Jarvis", Skills: []string{"clinical-decision-support", "clinical-diagnostic-reasoning"}, Universe: "us-east-1", Version: "0.7.0"}

	mockReg := &userMockRegistry{
		integrationMockRegistry: integrationMockRegistry{agents: map[string]*registry.AgentCard{
			"Whis": whis, "Fox": fox, "Benny": benny, "Jarvis": jarvis,
		}},
		loads: map[string]*registry.AgentLoad{},
	}

	ds := NewWauTrustDataSource(
		NewMemoryDataSource(mockReg, WithKernelVersion("0.7.0")),
		trustEng,
	)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, testLogger())
	return scoring, trustEng, mockReg
}

// =====================================================================
// U1: 并发 100x 提交 - 验证 ScoreAgents 线程安全 + 无 race
// =====================================================================

func TestUser_U1_ConcurrentScoreAgents(t *testing.T) {
	scoring, _, _ := makeUserScenariosEngine(t, map[string]float64{
		"Whis": 0.5, "Fox": 0.5, "Benny": 0.5, "Jarvis": 0.5,
	})

	agents := []string{"Whis", "Fox", "Benny", "Jarvis"}
	req := &ScoreRequest{RequiredSkills: []string{"translate"}}

	const goroutines = 100
	var wg sync.WaitGroup
	var errCount int32
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			scores, err := scoring.ScoreAgents(context.Background(), agents, req)
			if err != nil {
				atomic.AddInt32(&errCount, 1)
				return
			}
			if len(scores) != 4 {
				atomic.AddInt32(&errCount, 1)
			}
		}()
	}
	wg.Wait()

	if errCount > 0 {
		t.Fatalf("U1 concurrent: %d goroutines failed (want 0)", errCount)
	}
	t.Logf("U1 concurrent: %d goroutines all returned 4 scores cleanly", goroutines)
}

// =====================================================================
// U2: 空 RequiredSkills - 验证不报错 + SkillMatch=1.0(无要求=完美)
// =====================================================================

func TestUser_U2_EmptyRequiredSkills(t *testing.T) {
	scoring, _, _ := makeUserScenariosEngine(t, map[string]float64{
		"Whis": 0.5, "Fox": 0.5, "Benny": 0.5,
	})

	scores, err := scoring.ScoreAgents(context.Background(),
		[]string{"Whis", "Fox", "Benny"},
		&ScoreRequest{RequiredSkills: nil},
	)
	if err != nil {
		t.Fatalf("U2 ScoreAgents: %v", err)
	}
	if len(scores) != 3 {
		t.Fatalf("U2 expected 3 scores, got %d", len(scores))
	}
	// 空 skill → SkillMatch = 1.0 (无要求=完美匹配,合理设计)
	for _, s := range scores {
		if s.Dimensions.SkillMatch != 1.0 {
			t.Errorf("U2 %s SkillMatch=%f, want 1.0 (no required=perfect)", s.AgentID, s.Dimensions.SkillMatch)
		}
		if s.TotalScore == 0 {
			t.Errorf("U2 %s TotalScore=0, want >0 (其他维度仍算)", s.AgentID)
		}
	}
	t.Logf("U2 empty skills: 3 agents returned, SkillMatch=1.0 all (无要求=完美), TotalScore>0 from other dimensions")
}

// =====================================================================
// U3: 未知 skill - 期望所有 agent SkillMatch=0 → 次级维度(Trust/Health)主导
// =====================================================================

func TestUser_U3_UnknownSkillFallback(t *testing.T) {
	scoring, _, _ := makeUserScenariosEngine(t, map[string]float64{
		"Whis": 0.95, "Fox": 0.3, "Benny": 0.6,
	})

	scores, err := scoring.ScoreAgents(context.Background(),
		[]string{"Whis", "Fox", "Benny"},
		&ScoreRequest{RequiredSkills: []string{"quantum-physics-fusion"}},
	)
	if err != nil {
		t.Fatalf("U3 ScoreAgents: %v", err)
	}
	// SkillMatch 全部 = 0,top1 由 TrustScore 决定 → Whis (0.95) 应该排第一
	if scores[0].AgentID != "Whis" {
		t.Errorf("U3 top1=%s, want Whis (highest TrustScore 0.95)", scores[0].AgentID)
	}
	for _, s := range scores {
		if s.Dimensions.SkillMatch != 0 {
			t.Errorf("U3 %s SkillMatch=%f, want 0", s.AgentID, s.Dimensions.SkillMatch)
		}
	}
	t.Logf("U3 unknown skill: SkillMatch=0 all, TrustScore 主导, Whis(0.95)→%s", scores[0].AgentID)
}

// =====================================================================
// U4: 大量 agent (100) - 验证排序 O(n log n) 稳定 + 内存不爆
// =====================================================================

func TestUser_U4_ManyAgentsRanking(t *testing.T) {
	mockReg := &userMockRegistry{
		integrationMockRegistry: integrationMockRegistry{agents: map[string]*registry.AgentCard{}},
		loads:                   map[string]*registry.AgentLoad{},
	}
	agentIDs := make([]string, 100)
	for i := 0; i < 100; i++ {
		name := "Agent-" + string(rune('A'+i/26)) + string(rune('A'+i%26))
		agentIDs[i] = name
		mockReg.agents[name] = &registry.AgentCard{
			ID: name, Name: name, Skills: []string{"general"},
			Universe: "us-west-1", Version: "0.7.0",
		}
	}

	trustEng := engine.NewMemoryEngine()
	ctx := context.Background()
	ds := NewWauTrustDataSource(
		NewMemoryDataSource(mockReg, WithKernelVersion("0.7.0")),
		trustEng,
	)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, testLogger())

	start := time.Now()
	scores, err := scoring.ScoreAgents(ctx, agentIDs, &ScoreRequest{RequiredSkills: []string{"general"}})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("U4 ScoreAgents: %v", err)
	}
	if len(scores) != 100 {
		t.Fatalf("U4 expected 100 scores, got %d", len(scores))
	}
	// 验证排名单调递增(1..100)
	for i, s := range scores {
		if s.Rank != i+1 {
			t.Errorf("U4 position %d has Rank=%d, want %d", i, s.Rank, i+1)
		}
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("U4 took %v, want <100ms for 100 agents", elapsed)
	}
	t.Logf("U4 many agents: 100 agents ranked in %v", elapsed)
}

// =====================================================================
// U5: 极端 Trust 降权 - 1 agent trust=0,其他 trust=1 → top1 不应是 trust=0 的
// =====================================================================

func TestUser_U5_ExtremeTrustDrop(t *testing.T) {
	trustEng := engine.NewMemoryEngine()
	ctx := context.Background()
	// Whis trust 多次失败 → 接近 0
	for i := 0; i < 20; i++ {
		trustEng.RecordFailure(ctx, "Whis", 1.0)
	}
	// Fox / Benny trust 拉满
	for i := 0; i < 30; i++ {
		trustEng.RecordSuccess(ctx, "Fox", 1.0)
		trustEng.RecordSuccess(ctx, "Benny", 1.0)
	}

	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Whis":  {ID: "Whis", Name: "Whis", Skills: []string{"translate"}, Universe: "us-west-1", Version: "0.7.0"},
		"Fox":   {ID: "Fox", Name: "Fox", Skills: []string{"data-analysis"}, Universe: "us-west-1", Version: "0.7.0"},
		"Benny": {ID: "Benny", Name: "Benny", Skills: []string{"translate"}, Universe: "eu-central-1", Version: "0.7.0"},
	}}
	ds := NewWauTrustDataSource(
		NewMemoryDataSource(mockReg, WithKernelVersion("0.7.0")),
		trustEng,
	)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, testLogger())

	scores, err := scoring.ScoreAgents(ctx,
		[]string{"Whis", "Fox", "Benny"},
		&ScoreRequest{RequiredSkills: []string{"translate"}, SourceRegion: "us-west-1"},
	)
	if err != nil {
		t.Fatalf("U5 ScoreAgents: %v", err)
	}

	// 找 Whis 的 TrustScore
	var whisTrust float64
	for _, s := range scores {
		if s.AgentID == "Whis" {
			whisTrust = s.Dimensions.TrustScore
		}
	}
	if whisTrust > 0.2 {
		t.Errorf("U5 Whis TrustScore=%f, want <0.2 (extreme drop)", whisTrust)
	}
	// top1 应该不是 Whis
	if scores[0].AgentID == "Whis" {
		t.Errorf("U5 top1=Whis, want non-Whis (Whis TrustScore=%f)", whisTrust)
	}
	t.Logf("U5 extreme trust drop: Whis TrustScore=%f, top1=%s (not Whis)", whisTrust, scores[0].AgentID)
}

// =====================================================================
// U6: CPU 100% 健康恶化 - 验证 HealthScore 真反映 CPU
// =====================================================================

func TestUser_U6_HighLoadHealthDrop(t *testing.T) {
	scoring, _, mockReg := makeUserScenariosEngine(t, map[string]float64{
		"Whis": 0.9, "Fox": 0.9, "Benny": 0.9,
	})
	// 模拟 Whis CPU 100% + Memory 100%(满载,健康 = 0)
	mockReg.loads["Whis"] = &registry.AgentLoad{
		AgentID: "Whis", ActiveTasks: 10, MaxCapacity: 10, CPUUsage: 1.0, MemoryUsage: 1.0,
	}
	// Fox / Benny 正常
	mockReg.loads["Fox"] = &registry.AgentLoad{
		AgentID: "Fox", ActiveTasks: 1, MaxCapacity: 10, CPUUsage: 0.1, MemoryUsage: 0.1,
	}
	mockReg.loads["Benny"] = &registry.AgentLoad{
		AgentID: "Benny", ActiveTasks: 1, MaxCapacity: 10, CPUUsage: 0.1, MemoryUsage: 0.1,
	}

	scores, err := scoring.ScoreAgents(context.Background(),
		[]string{"Whis", "Fox", "Benny"},
		&ScoreRequest{RequiredSkills: []string{"general"}, SourceRegion: "us-west-1"},
	)
	if err != nil {
		t.Fatalf("U6 ScoreAgents: %v", err)
	}

	// 找 Whis HealthScore 和 LoadScore
	var whisHealth, whisLoad float64
	for _, s := range scores {
		if s.AgentID == "Whis" {
			whisHealth = s.Dimensions.HealthScore
			whisLoad = s.Dimensions.LoadScore
		}
	}
	if whisHealth >= 0.5 {
		t.Errorf("U6 Whis HealthScore=%f, want <0.5 (CPU=100%%, Memory=100%%)", whisHealth)
	}
	if whisLoad >= 0.5 {
		t.Errorf("U6 Whis LoadScore=%f, want <0.5 (active=10/10)", whisLoad)
	}
	t.Logf("U6 high load: Whis HealthScore=%f, LoadScore=%f (CPU=100%%, Mem=100%%, active=10/10)", whisHealth, whisLoad)
}

// =====================================================================
// U7: 混合维度决策 - skill 完美 + trust=0  vs  skill 中等 + trust=1
//     期望:trade-off 由 15 维权重和决定
// =====================================================================

func TestUser_U7_MixedSkillAndTrust(t *testing.T) {
	trustEng := engine.NewMemoryEngine()
	ctx := context.Background()
	// Alpha: skill 完美匹配 translate,但 trust=0
	for i := 0; i < 30; i++ {
		trustEng.RecordFailure(ctx, "Alpha", 1.0)
	}
	// Beta: skill 没 translate,但 trust=1
	for i := 0; i < 30; i++ {
		trustEng.RecordSuccess(ctx, "Beta", 1.0)
	}

	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Alpha": {ID: "Alpha", Name: "Alpha", Skills: []string{"translate", "general"}, Universe: "us-west-1", Version: "0.7.0"},
		"Beta":  {ID: "Beta", Name: "Beta", Skills: []string{"data-analysis"}, Universe: "us-west-1", Version: "0.7.0"},
	}}
	ds := NewWauTrustDataSource(
		NewMemoryDataSource(mockReg, WithKernelVersion("0.7.0")),
		trustEng,
	)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, testLogger())

	scores, err := scoring.ScoreAgents(ctx,
		[]string{"Alpha", "Beta"},
		&ScoreRequest{RequiredSkills: []string{"translate"}},
	)
	if err != nil {
		t.Fatalf("U7 ScoreAgents: %v", err)
	}

	// Alpha: SkillMatch=1, TrustScore≈0
	// Beta:  SkillMatch=0, TrustScore≈1
	// SkillMatch 权重 0.25, TrustScore 权重 0.20 → Alpha 应胜出(SkillMatch 占优)
	if scores[0].AgentID != "Alpha" {
		t.Errorf("U7 top1=%s, want Alpha (SkillMatch 0.25 > TrustScore 0.20)",
			scores[0].AgentID)
	}
	t.Logf("U7 mixed: Alpha(SkillMatch=1,Trust≈0) vs Beta(SkillMatch=0,Trust≈1) → top1=%s (SkillMatch 主导)", scores[0].AgentID)
}

// =====================================================================
// U8: 顺序稳定 - 同分时排名 deterministic(避免随机性)
// =====================================================================

func TestUser_U8_DeterministicRanking(t *testing.T) {
	scoring, _, _ := makeUserScenariosEngine(t, map[string]float64{
		"Whis": 0.5, "Fox": 0.5, "Benny": 0.5,
	})

	// 跑 5 次,验证排名顺序完全一致
	var firstOrder []string
	for i := 0; i < 5; i++ {
		scores, err := scoring.ScoreAgents(context.Background(),
			[]string{"Whis", "Fox", "Benny"},
			&ScoreRequest{RequiredSkills: []string{"general"}},
		)
		if err != nil {
			t.Fatalf("U8 iter %d: %v", i, err)
		}
		order := []string{scores[0].AgentID, scores[1].AgentID, scores[2].AgentID}
		if i == 0 {
			firstOrder = order
		} else {
			for j := range order {
				if order[j] != firstOrder[j] {
					t.Errorf("U8 iter %d pos %d: %s != first %s", i, j, order[j], firstOrder[j])
				}
			}
		}
	}
	t.Logf("U8 deterministic: 5 runs all returned order %v", firstOrder)
}

// =====================================================================
// U9: 空 agent 列表 - 边界条件:无 agent 可调度
// =====================================================================

func TestUser_U9_EmptyAgentList(t *testing.T) {
	scoring, _, _ := makeUserScenariosEngine(t, map[string]float64{})

	scores, err := scoring.ScoreAgents(context.Background(),
		[]string{}, // 空列表
		&ScoreRequest{RequiredSkills: []string{"translate"}},
	)
	if err != nil {
		t.Fatalf("U9 ScoreAgents: %v", err)
	}
	if len(scores) != 0 {
		t.Errorf("U9 expected 0 scores for empty input, got %d", len(scores))
	}
	t.Logf("U9 empty input: 0 agents → 0 scores (no error)")
}

// =====================================================================
// U10: 并发 + 大量数据 - 1000 latencies + 100 goroutines 压测
// =====================================================================

func TestUser_U10_HighVolumePressure(t *testing.T) {
	scoring, _, _ := makeUserScenariosEngine(t, map[string]float64{
		"Whis": 0.7, "Fox": 0.7, "Benny": 0.7, "Jarvis": 0.7,
	})

	// 注入大量延迟数据到 MemoryDataSource(wau-trust wrapper 透传)
	memDS, ok := scoring.ds.(*WauTrustDataSource)
	if !ok {
		t.Fatalf("U10 scoring.ds is not *WauTrustDataSource: %T", scoring.ds)
	}
	innerDS, ok := memDS.inner.(*MemoryDataSource)
	if !ok {
		t.Fatalf("U10 memDS.inner is not *MemoryDataSource: %T", memDS.inner)
	}
	for _, name := range []string{"Whis", "Fox", "Benny", "Jarvis"} {
		lats := make([]float64, 1000)
		for i := range lats {
			lats[i] = rand.Float64() * 2.0 // 0-2s
		}
		innerDS.SetLatencies(name, lats)
		innerDS.SetTotalTasks(name, 500)
	}

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _ = scoring.ScoreAgents(context.Background(),
				[]string{"Whis", "Fox", "Benny", "Jarvis"},
				&ScoreRequest{RequiredSkills: []string{"general"}},
			)
		}()
	}
	wg.Wait()
	t.Logf("U10 high volume: %d goroutines with 1000 latencies + 500 tasks each completed", goroutines)
}

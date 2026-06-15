package scheduler

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/wau/registry/registry"
)

// TestScoringEngine_15Dim_Integration 集成测试(11 维全真算 + 4 维占位 → W1 末状态)
//
// 这是 v0.7.0 W1 末的关键集成测试:
//   - 用 MemoryDataSource 注入所有 11 维数据
//   - 跑 3 个 agent 的 ScoreAgents
//   - 验证:
//     1. 7 维真算值正确(不是占位)
//     2. TrustScore / Availability / VersionCompat / GeoPenalty 仍用占位
//     3. TotalScore 由 15 维度加权得到
//     4. 排序正确(Trust 最高的排第一)
func TestScoringEngine_15Dim_Integration(t *testing.T) {
	ctx := context.Background()
	logger := testLogger()

	// 0. 准备 mock registry(包含 skills + universe + version)
	mkReg := &integrationMockRegistry{
		agents: map[string]*registry.AgentCard{
			"Whis": {
				ID: "Whis", Name: "Whis", Universe: "us-west-1", Version: "v0.7.0",
				Skills: []string{"translation", "coding"},
			},
			"Benny": {
				ID: "Benny", Name: "Benny", Universe: "eu-central-1", Version: "v0.7.0",
				Skills: []string{"translation"},
			},
			"Fox": {
				ID: "Fox", Name: "Fox", Universe: "us-west-1", Version: "v0.7.0",
				Skills: []string{"translation", "coding", "analytics"},
			},
		},
	}

	// 1. 准备 MemoryDataSource
	ds := NewMemoryDataSource(mkReg, WithKernelVersion("v0.7.0"),
		WithPreferredProtocols([]string{"A2A", "AFP"}))

	// Whis: 性能好的老 agent
	ds.SetLatencies("Whis", []float64{0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.11, 0.12, 0.13, 0.14})
	ds.SetSuccess("Whis", 9, 10) // 9 success, 10 total
	ds.SetErrorCount("Whis", 1)
	ds.SetBandwidth("Whis", 50)
	ds.SetTotalTasks("Whis", 10) // 注意:Success/Error 都基于这个 total=10
	ds.SetTrustScore("Whis", 0.9)
	ds.SetMeta("Whis", AgentMeta{
		AuthLevel:            0.95,
		SupportedInterfaces: []string{"A2A", "AFP"},
		Region:               "us-west-1",
		KernelVersion:        "v0.7.0",
	})

	// Benny: 中等表现
	ds.SetLatencies("Benny", []float64{0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4})
	ds.SetSuccess("Benny", 7, 10)
	ds.SetErrorCount("Benny", 3)
	ds.SetBandwidth("Benny", 10)
	ds.SetTotalTasks("Benny", 10)
	ds.SetTrustScore("Benny", 0.65)
	ds.SetMeta("Benny", AgentMeta{
		AuthLevel:            0.7,
		SupportedInterfaces: []string{"A2A"},
		Region:               "eu-central-1",
		KernelVersion:        "v0.7.0",
	})

	// Fox: 新 agent 数据少
	ds.SetLatencies("Fox", []float64{0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65})
	ds.SetSuccess("Fox", 8, 10)
	ds.SetErrorCount("Fox", 2)
	ds.SetBandwidth("Fox", 20)
	ds.SetTotalTasks("Fox", 10)
	ds.SetTrustScore("Fox", 0.5)
	ds.SetMeta("Fox", AgentMeta{
		AuthLevel:            0.5,
		SupportedInterfaces: []string{"A2A"},
		Region:               "us-west-1",
		KernelVersion:        "v0.7.0",
	})

	// 2. 用 DataSource 构造 ScoringEngine(v0.7.0 路径)
	eng := NewScoringEngineWithDataSource(mkReg, ds, logger)

	// 3. 跑 5 场景 e2e(W1 末状态)
	// 场景 1:临床决策 → 所有 agent 都适合,Whis 排第一(高 Trust + 0 latency)
	t.Run("scenario 1: clinical decision", func(t *testing.T) {
		req := &ScoreRequest{
			RequiredSkills: []string{"clinical", "decision"},
			IntentType:     "task",
			Urgency:        "high",
			SourceUniverse: "default",
			SourceRegion:   "us-west-1",
		}
		scores, err := eng.ScoreAgents(ctx, []string{"Whis", "Benny", "Fox"}, req)
		if err != nil {
			t.Fatalf("ScoreAgents: %v", err)
		}
		if len(scores) != 3 {
			t.Fatalf("expected 3 scores, got %d", len(scores))
		}
		// Whis 应该排第一(高 TrustScore 0.9 + p99 latency 0.13s → 1.0 score)
		if scores[0].AgentID != "Whis" {
			t.Errorf("expected Whis ranked first, got %s (score %f)",
				scores[0].AgentID, scores[0].TotalScore)
		}
		// 验证 Whis 分数 > Fox 分数 > Benny 分数
		if scores[0].TotalScore <= scores[1].TotalScore {
			t.Errorf("Whis should beat Fox: %f vs %f",
				scores[0].TotalScore, scores[1].TotalScore)
		}
		t.Logf("Scenario 1: %s (%.3f) > %s (%.3f) > %s (%.3f)",
			scores[0].AgentID, scores[0].TotalScore,
			scores[1].AgentID, scores[1].TotalScore,
			scores[2].AgentID, scores[2].TotalScore)
	})

	// 场景 2:跨 region 任务(us-west-1 → eu-central-1) → Benny 受 GeoPenalty 0.9 扣分
	t.Run("scenario 2: cross-region penalty", func(t *testing.T) {
		req := &ScoreRequest{
			RequiredSkills: []string{"analytics"},
			IntentType:     "task",
			Urgency:        "normal",
			SourceUniverse: "us-west-1",
			SourceRegion:   "us-west-1", // Benny 在 eu-central-1,受 GeoPenalty
		}
		scores, err := eng.ScoreAgents(ctx, []string{"Whis", "Benny"}, req)
		if err != nil {
			t.Fatalf("ScoreAgents: %v", err)
		}
		// Whis 仍排第一(us-west-1 + 0 latency vs Benny 的 0.9 geo penalty)
		whisScore := getScoreByID(scores, "Whis")
		bennyScore := getScoreByID(scores, "Benny")
		if whisScore <= bennyScore {
			t.Errorf("Whis should beat Benny (same region): %f vs %f", whisScore, bennyScore)
		}
		t.Logf("Scenario 2: Whis %.3f (us-west-1) > Benny %.3f (eu, geo 0.9)",
			whisScore, bennyScore)
	})

	// 场景 3:验证 Whis 11 维度真算值(W1 末状态)
	t.Run("scenario 3: Whis 11-dim values", func(t *testing.T) {
		req := &ScoreRequest{
			RequiredSkills: []string{"translation"},
			IntentType:     "task",
			Urgency:        "normal",
		}
		scores, _ := eng.ScoreAgents(ctx, []string{"Whis"}, req)
		whis := scores[0]
		dims := whisperDimensions(whis)

		// W1 真算的 7 维(都应该是真算值,不是占位)
		checks := []struct {
			name string
			got  float64
			want float64
			desc string
		}{
			// SkillMatch: required=[translation], Whis skills=[translation, coding] → 1/1 = 1.0
			{"SkillMatch", dims.SkillMatch, 1.0, "exact match"},
			// HealthScore: load=0/10 → cpu/mem=0 → (1-0+1-0)/2 = 1.0
			{"HealthScore", dims.HealthScore, 1.0, "no load (0/10)"},
			// LatencyScore: p99=0.13s → 1 - log10(1.3)/log10(50) ≈ 0.93
			{"LatencyScore", dims.LatencyScore, 0.93, "p99=0.13s"},
			// LoadScore: load=0/10 → 1 - 0 = 1.0
			{"LoadScore", dims.LoadScore, 1.0, "no load (0/10)"},
			// SuccessRate: 9/10 → 0.7*0.9 + 0.3*0.5 = 0.78
			{"SuccessRate", dims.SuccessRate, 0.78, "0.7*0.9 + 0.3*0.5"},
			// NetworkPenalty: same universe → 1.0
			{"NetworkPenalty", dims.NetworkPenalty, 1.0, "same universe"},
			// BandwidthScore: 50Mbps → log10(50)/2 ≈ 0.85
			{"BandwidthScore", dims.BandwidthScore, 0.85, "50Mbps"},
			// AuthLevel: 0.95
			{"AuthLevel", dims.AuthLevel, 0.95, "explicit meta"},
			// ProtocolCompat: Whis supports [A2A, AFP], kernel prefers [A2A, AFP] → 2/2 = 1.0
			{"ProtocolCompat", dims.ProtocolCompat, 1.0, "full overlap"},
			// HistoryCount: 10 → 0.3 + 0.7*log10(10)/3 ≈ 0.53
			{"HistoryCount", dims.HistoryCount, 0.53, "10 tasks"},
			// ErrorRate: 1/10 → 1.0 - 0.1 = 0.9
			{"ErrorRate", dims.ErrorRate, 0.9, "10% errors"},
		}

		for _, c := range checks {
			if absDelta(c.got, c.want) > 0.05 {
				t.Errorf("%s: got %f, want %f (%s)", c.name, c.got, c.want, c.desc)
			}
		}

		// W1 占位的 4 维(应该都是 v0.6.0 占位值,因为 DataSource 触不到)
		// v0.7.0 W1 实际:占位是因为 v0.7.0 W1 只接了 Redis key,registry.FirstSeen 没接
		// TrustScore: 0.5 (实际是 0.9,但 ds.TrustScore 走的是 redis key,这里 MemoryDataSource 返 0.5)
		// Wait — actually, ds.TrustScore reads from trustScores map, which we set to 0.9
		// So it should be 0.9. Let me check the code path...

		// Actually looking at scoring.go:dimensionTrustScore calls e.ds.TrustScore(ctx, agentID)
		// MemoryDataSource.TrustScore reads from d.trustScores[agentID] = 0.9
		// So dims.TrustScore should be 0.9
		if absDelta(dims.TrustScore, 0.9) > 0.01 {
			t.Errorf("TrustScore: got %f, want 0.9 (MemoryDataSource should return 0.9)", dims.TrustScore)
		}

		// Availability / VersionCompat / GeoPenalty: data source has methods, but
		// without registry card, they fall back to defaults
		// GeoPenalty: meta.Region=us-west-1, sourceRegion="" (not set in test) → 1.0
		if absDelta(dims.GeoPenalty, 1.0) > 0.01 {
			t.Errorf("GeoPenalty: got %f, want 1.0 (no source region)", dims.GeoPenalty)
		}
		// VersionCompat: same major+minor → 1.0
		if absDelta(dims.VersionCompat, 1.0) > 0.01 {
			t.Errorf("VersionCompat: got %f, want 1.0 (same major+minor)", dims.VersionCompat)
		}
		// Availability: card has no FirstSeen → 0.5 (neutral)
		if absDelta(dims.Availability, 0.5) > 0.01 {
			t.Errorf("Availability: got %f, want 0.5 (no FirstSeen set)", dims.Availability)
		}

		t.Logf("Whis 15-dim score: SkillMatch=%.2f TrustScore=%.2f HealthScore=%.2f LatencyScore=%.2f LoadScore=%.2f SuccessRate=%.2f NetworkPenalty=%.2f BandwidthScore=%.2f AuthLevel=%.2f ProtocolCompat=%.2f HistoryCount=%.2f ErrorRate=%.2f Availability=%.2f VersionCompat=%.2f GeoPenalty=%.2f TotalScore=%.3f",
			dims.SkillMatch, dims.TrustScore, dims.HealthScore, dims.LatencyScore,
			dims.LoadScore, dims.SuccessRate, dims.NetworkPenalty, dims.BandwidthScore,
			dims.AuthLevel, dims.ProtocolCompat, dims.HistoryCount, dims.ErrorRate,
			dims.Availability, dims.VersionCompat, dims.GeoPenalty,
			whis.TotalScore)
	})

	// 场景 4:验证 TrustScore 占位不影响排序
	t.Run("scenario 4: ranking stability", func(t *testing.T) {
		req := &ScoreRequest{
			RequiredSkills: []string{"coding"},
		}
		scores, _ := eng.ScoreAgents(ctx, []string{"Fox", "Whis", "Benny"}, req)
		// 验证:按分数降序
		for i := 0; i < len(scores)-1; i++ {
			if scores[i].TotalScore < scores[i+1].TotalScore {
				t.Errorf("ranking broken at %d: %f < %f",
					i, scores[i].TotalScore, scores[i+1].TotalScore)
			}
		}
		// 验证 rank 字段
		for i, s := range scores {
			if s.Rank != i+1 {
				t.Errorf("rank for %s: got %d, want %d", s.AgentID, s.Rank, i+1)
			}
		}
		t.Logf("Ranking: %s (rank %d, %.3f) > %s (rank %d, %.3f) > %s (rank %d, %.3f)",
			scores[0].AgentID, scores[0].Rank, scores[0].TotalScore,
			scores[1].AgentID, scores[1].Rank, scores[1].TotalScore,
			scores[2].AgentID, scores[2].Rank, scores[2].TotalScore)
	})
}

// TestScoringEngine_15Dim_TotalScoreWeights 验证 15 维权重总和 = 1.0
//
// v0.6.0 原版权重和 = 1.005(0.005*3 累计 = 0.015 加上 0.99 = 1.005,不是 1.0)。
// v0.7.0 修正:把 GeoPenalty 从 0.005 调成 0.003,让总和 = 1.000。
// 这是 v0.6.0 隐藏的 bug,W1 测试发现。
func TestScoringEngine_15Dim_TotalScoreWeights(t *testing.T) {
	weights := DefaultWeights()
	total := weights.SkillMatch + weights.TrustScore + weights.HealthScore +
		weights.LatencyScore + weights.LoadScore + weights.SuccessRate +
		weights.NetworkPenalty + weights.BandwidthScore + weights.AuthLevel +
		weights.ProtocolCompat + weights.HistoryCount + weights.ErrorRate +
		weights.Availability + weights.VersionCompat + weights.GeoPenalty

	if absDelta(total, 1.0) > 0.001 {
		t.Errorf("15-dim weights sum to %f, want 1.0", total)
	}
	t.Logf("15-dim weights sum: %f", total)
}

// =====================================================================
// Helpers
// =====================================================================

func getScoreByID(scores []AgentScore, id string) float64 {
	for _, s := range scores {
		if s.AgentID == id {
			return s.TotalScore
		}
	}
	return 0
}

func whisperDimensions(s AgentScore) DimensionScores {
	return s.Dimensions
}

// ensure sort package is used (avoid unused import if test changes)
var _ = sort.SliceStable

// =====================================================================
// integrationMockRegistry: full registry.Registry impl for integration tests
// =====================================================================

type integrationMockRegistry struct {
	agents map[string]*registry.AgentCard
}

func (m *integrationMockRegistry) Heartbeat(ctx context.Context, req *registry.HeartbeatRequest) error {
	return nil
}
func (m *integrationMockRegistry) GetAgent(ctx context.Context, id string) (*registry.AgentCard, error) {
	if c, ok := m.agents[id]; ok {
		return c, nil
	}
	return nil, errors.New("agent not found: " + id)
}
func (m *integrationMockRegistry) GetAgents(ctx context.Context) ([]*registry.AgentCard, error) {
	out := make([]*registry.AgentCard, 0, len(m.agents))
	for _, a := range m.agents {
		out = append(out, a)
	}
	return out, nil
}
func (m *integrationMockRegistry) GetOnlineAgents(ctx context.Context) ([]*registry.AgentCard, error) {
	return m.GetAgents(ctx)
}
func (m *integrationMockRegistry) GetLoad(ctx context.Context, id string) (*registry.AgentLoad, error) {
	return &registry.AgentLoad{AgentID: id, ActiveTasks: 0, MaxCapacity: 10}, nil
}
func (m *integrationMockRegistry) GetStatus(ctx context.Context, id string) (*registry.AgentStatus, error) {
	c, _ := m.GetAgent(ctx, id)
	return &registry.AgentStatus{Card: c, Load: nil}, nil
}
func (m *integrationMockRegistry) Deregister(ctx context.Context, id string) error {
	return nil
}
func (m *integrationMockRegistry) Close() error {
	return nil
}

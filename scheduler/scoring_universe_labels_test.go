package scheduler

import (
	"context"
	"testing"

	"github.com/wau/registry/registry"
)

// mockRegistryForLabelTest 最小化 mock registry,只实现 GetAgent
// 让 scoring.go 拿得到 UniverseLabels
type mockRegistryForLabelTest struct {
	agents map[string]*registry.AgentCard
}

func (m *mockRegistryForLabelTest) Heartbeat(ctx context.Context, req *registry.HeartbeatRequest) error {
	return nil
}
func (m *mockRegistryForLabelTest) GetAgent(ctx context.Context, id string) (*registry.AgentCard, error) {
	if c, ok := m.agents[id]; ok {
		return c, nil
	}
	return nil, nil
}
func (m *mockRegistryForLabelTest) GetAgents(ctx context.Context) ([]*registry.AgentCard, error) {
	out := make([]*registry.AgentCard, 0, len(m.agents))
	for _, a := range m.agents {
		out = append(out, a)
	}
	return out, nil
}
func (m *mockRegistryForLabelTest) GetOnlineAgents(ctx context.Context) ([]*registry.AgentCard, error) {
	return m.GetAgents(ctx)
}
func (m *mockRegistryForLabelTest) GetLoad(ctx context.Context, id string) (*registry.AgentLoad, error) {
	return &registry.AgentLoad{AgentID: id, MaxCapacity: 10}, nil
}
func (m *mockRegistryForLabelTest) GetStatus(ctx context.Context, id string) (*registry.AgentStatus, error) {
	c, _ := m.GetAgent(ctx, id)
	return &registry.AgentStatus{Card: c}, nil
}
func (m *mockRegistryForLabelTest) Deregister(ctx context.Context, id string) error { return nil }
func (m *mockRegistryForLabelTest) Close() error                                   { return nil }

// TestCalcNetworkPenalty_LabelAware 验证 calcNetworkPenalty label 加权逻辑(v0.8.0 M5-1 B.3)
//
// 决策表:
//   - sourceUniverse == "" → 1.0
//   - 跨 universe → 0.7
//   - 同 universe + 同 region → 1.0
//   - 同 universe + 跨 region → 0.95
//   - 同 universe + 一方缺 region label → 1.0(降级,避免误判)
func TestCalcNetworkPenalty_LabelAware(t *testing.T) {
	e := &ScoringEngine{logger: testLogger()}

	tests := []struct {
		name         string
		agentUni     string
		agentLabels  map[string]string
		sourceUni    string
		sourceLabels map[string]string
		want         float64
	}{
		{
			name:      "empty source → 1.0 (老行为)",
			agentUni:  "us-west-1",
			sourceUni: "",
			want:      1.0,
		},
		{
			name:      "跨 universe,无 label → 0.7 (老行为)",
			agentUni:  "us-west-1",
			sourceUni: "eu-central-1",
			want:      0.7,
		},
		{
			name:      "跨 universe,同 region label → 仍然 0.7 (跨 universe 优先)",
			agentUni:  "us-west-1",
			agentLabels: map[string]string{"region": "us-west-1"},
			sourceUni:    "eu-central-1",
			sourceLabels: map[string]string{"region": "us-west-1"},
			want:         0.7,
		},
		{
			name:      "同 universe,同 region → 1.0",
			agentUni:  "us-west-1",
			agentLabels: map[string]string{"region": "us-west-1"},
			sourceUni:    "us-west-1",
			sourceLabels: map[string]string{"region": "us-west-1"},
			want:         1.0,
		},
		{
			name:      "同 universe,跨 region → 0.95 (新行为)",
			agentUni:  "us-west-1",
			agentLabels: map[string]string{"region": "us-west-1"},
			sourceUni:    "us-west-1",
			sourceLabels: map[string]string{"region": "eu-central-1"},
			want:         0.95,
		},
		{
			name:         "同 universe,agent 缺 label → 1.0 (降级)",
			agentUni:     "us-west-1",
			agentLabels:  nil,
			sourceUni:    "us-west-1",
			sourceLabels: map[string]string{"region": "us-west-1"},
			want:         1.0,
		},
		{
			name:         "同 universe,source 缺 label → 1.0 (降级)",
			agentUni:     "us-west-1",
			agentLabels:  map[string]string{"region": "us-west-1"},
			sourceUni:    "us-west-1",
			sourceLabels: nil,
			want:         1.0,
		},
		{
			name:        "同 universe,都缺 label → 1.0 (回退到 v0.7 老行为)",
			agentUni:    "us-west-1",
			sourceUni:   "us-west-1",
			want:        1.0,
		},
		{
			name:      "多 label,只看 region → 1.0 (其他 label 忽略)",
			agentUni:  "us-west-1",
			agentLabels: map[string]string{
				"region": "us-west-1",
				"tier":   "high-performance",
				"gpu":    "true",
			},
			sourceUni: "us-west-1",
			sourceLabels: map[string]string{
				"region": "us-west-1",
				"tier":   "low", // 不同,但不影响
			},
			want: 1.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := e.calcNetworkPenalty(tc.agentUni, tc.agentLabels, tc.sourceUni, tc.sourceLabels)
			if got != tc.want {
				t.Errorf("calcNetworkPenalty(%q, %v, %q, %v) = %f, want %f",
					tc.agentUni, tc.agentLabels, tc.sourceUni, tc.sourceLabels, got, tc.want)
			}
		})
	}
}

// TestScoreAgents_UniverseLabels_Routing 集成测试:同 universe 跨 region 时,
// 标了正确 region label 的 agent 排第一
func TestScoreAgents_UniverseLabels_Routing(t *testing.T) {
	ctx := context.Background()
	logger := testLogger()

	mkReg := &mockRegistryForLabelTest{
		agents: map[string]*registry.AgentCard{
			"agent-west": {
				ID: "agent-west", Name: "agent-west",
				Universe: "us-west-1",
				UniverseLabels: map[string]string{
					"region": "us-west-1",
					"tier":   "high-performance",
				},
			},
			"agent-east": {
				ID: "agent-east", Name: "agent-east",
				Universe: "us-west-1", // 同 universe(都属 us-west-1 universe group)
				UniverseLabels: map[string]string{
					"region": "us-east-1", // 跨 region
					"tier":   "high-performance",
				},
			},
		},
	}

	// 用 nil DataSource → 走 v0.7.x 占位,只观察 NetworkPenalty 差异
	eng := NewScoringEngine(mkReg, logger)

	req := &ScoreRequest{
		SourceUniverse: "us-west-1",
		SourceUniverseLabels: map[string]string{
			"region": "us-west-1",
		},
	}

	scores, err := eng.ScoreAgents(ctx, []string{"agent-west", "agent-east"}, req)
	if err != nil {
		t.Fatalf("ScoreAgents: %v", err)
	}
	if len(scores) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(scores))
	}

	// agent-west(同 region)应排第一
	if scores[0].AgentID != "agent-west" {
		t.Errorf("expected agent-west ranked first, got %s (score %f)",
			scores[0].AgentID, scores[0].TotalScore)
	}
	// 验证 NetworkPenalty 维度值(agent-west=1.0, agent-east=0.95)
	westNet := getDimByID(scores, "agent-west", func(s AgentScore) float64 { return s.Dimensions.NetworkPenalty })
	eastNet := getDimByID(scores, "agent-east", func(s AgentScore) float64 { return s.Dimensions.NetworkPenalty })
	if westNet <= eastNet {
		t.Errorf("agent-west NetworkPenalty should beat agent-east: %f vs %f", westNet, eastNet)
	}
	// TotalScore:差距 = 0.05 * 0.05 = 0.0025
	westTotal := getDimByID(scores, "agent-west", func(s AgentScore) float64 { return s.TotalScore })
	eastTotal := getDimByID(scores, "agent-east", func(s AgentScore) float64 { return s.TotalScore })
	if westTotal <= eastTotal {
		t.Errorf("agent-west TotalScore should beat agent-east: %f vs %f", westTotal, eastTotal)
	}
	t.Logf("agent-west (region=us-west-1) NetPenalty=%.2f Total=%.4f > agent-east (region=us-east-1) NetPenalty=%.2f Total=%.4f",
		westNet, westTotal, eastNet, eastTotal)
}

// getDimByID helper:按 ID 找 agent,提取维度值
func getDimByID(scores []AgentScore, id string, extractor func(AgentScore) float64) float64 {
	for _, s := range scores {
		if s.AgentID == id {
			return extractor(s)
		}
	}
	return 0
}

package deadlock

import (
	"context"
	"testing"
	"time"
)

func TestWaitForGraph_HasCycles_NoCycle(t *testing.T) {
	// A → B → C (无环)
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")

	has, deadlocked := g.HasCycles()
	if has {
		t.Errorf("expected no cycle, got cycle: %v", deadlocked)
	}
}

func TestWaitForGraph_HasCycles_TwoNodes(t *testing.T) {
	// A ↔ B (环)
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.AddEdge("B", "A")

	has, deadlocked := g.HasCycles()
	if !has {
		t.Fatal("expected cycle")
	}
	if len(deadlocked) != 2 {
		t.Errorf("expected 2 deadlocked nodes, got %d: %v", len(deadlocked), deadlocked)
	}
}

func TestWaitForGraph_HasCycles_ThreeNodes(t *testing.T) {
	// A → B → C → A (环)
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	g.AddEdge("C", "A")

	has, deadlocked := g.HasCycles()
	if !has {
		t.Fatal("expected cycle")
	}
	if len(deadlocked) != 3 {
		t.Errorf("expected 3 deadlocked nodes, got %d: %v", len(deadlocked), deadlocked)
	}
}

func TestWaitForGraph_HasCycles_DisconnectedWithCycle(t *testing.T) {
	// A → B(无环),C ↔ D(有环)
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.AddEdge("C", "D")
	g.AddEdge("D", "C")

	has, deadlocked := g.HasCycles()
	if !has {
		t.Fatal("expected cycle")
	}
	if len(deadlocked) != 2 {
		t.Errorf("expected 2 deadlocked nodes, got %d: %v", len(deadlocked), deadlocked)
	}
}

func TestWaitForGraph_RemoveEdge(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.RemoveEdge("A", "B")

	has, _ := g.HasCycles()
	if has {
		t.Error("should not have cycle after removing edge")
	}
}

func TestWaitForGraph_RemoveNode(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.AddEdge("B", "A")
	g.RemoveNode("A")

	has, _ := g.HasCycles()
	if has {
		t.Error("should not have cycle after removing node A")
	}
	if g.EdgeCount() != 0 {
		t.Errorf("expected 0 edges, got %d", g.EdgeCount())
	}
}

func TestWaitForGraph_SelfLoopIgnored(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge("A", "A") // 自环

	has, _ := g.HasCycles()
	if has {
		t.Error("self-loop should not count as deadlock")
	}
}

func TestWaitForGraph_Nodes(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.AddEdge("C", "D")

	nodes := g.Nodes()
	if len(nodes) != 4 {
		t.Errorf("expected 4 nodes, got %d: %v", len(nodes), nodes)
	}
}

func TestDetector_DryRun_NoKill(t *testing.T) {
	cfg := DetectorConfig{
		Threshold:     10 * time.Millisecond,
		CheckInterval: 1 * time.Second,
		DryRun:        true,
	}
	mock := NewMockTrustGetter()
	mock.SetScore("A", 0.3)
	mock.SetScore("B", 0.7)

	det := NewDetector(cfg, mock, nil)

	// 建死锁
	det.RecordEdge("A", "B")
	det.RecordEdge("B", "A")

	// 等过 threshold
	time.Sleep(50 * time.Millisecond)

	victims := det.CheckOnce(context.Background())
	if victims != nil {
		t.Errorf("dry-run should return nil, got: %v", victims)
	}
}

func TestDetector_RealKill_PicksWeakest(t *testing.T) {
	cfg := DetectorConfig{
		Threshold:     10 * time.Millisecond,
		CheckInterval: 1 * time.Second,
		DryRun:        false, // 真杀模式
	}
	mock := NewMockTrustGetter()
	mock.SetScore("A", 0.7) // 强
	mock.SetScore("B", 0.3) // 弱 — 应该被选中
	mock.SetScore("C", 0.5)

	det := NewDetector(cfg, mock, nil)

	// 三方互等
	det.RecordEdge("A", "B")
	det.RecordEdge("B", "C")
	det.RecordEdge("C", "A")

	time.Sleep(50 * time.Millisecond)

	victims := det.CheckOnce(context.Background())
	if len(victims) != 1 {
		t.Fatalf("expected 1 victim, got %d: %v", len(victims), victims)
	}
	if victims[0] != "B" {
		t.Errorf("expected weakest trust 'B' (0.3), got '%s'", victims[0])
	}
}

func TestDetector_RealKill_TieBreakerByName(t *testing.T) {
	cfg := DetectorConfig{
		Threshold:     10 * time.Millisecond,
		CheckInterval: 1 * time.Second,
		DryRun:        false,
	}
	mock := NewMockTrustGetter()
	mock.SetScore("Z", 0.5)
	mock.SetScore("A", 0.5) // 同分,字典序最小

	det := NewDetector(cfg, mock, nil)

	det.RecordEdge("Z", "A")
	det.RecordEdge("A", "Z")

	time.Sleep(50 * time.Millisecond)

	victims := det.CheckOnce(context.Background())
	if len(victims) != 1 {
		t.Fatalf("expected 1 victim")
	}
	if victims[0] != "A" {
		t.Errorf("expected tie-break winner 'A', got '%s'", victims[0])
	}
}

func TestDetector_NoCycle(t *testing.T) {
	cfg := DefaultDetectorConfig()
	cfg.Threshold = 10 * time.Millisecond
	cfg.DryRun = false

	det := NewDetector(cfg, NewMockTrustGetter(), nil)

	det.RecordEdge("A", "B")
	det.RecordEdge("B", "C")
	// 无环

	time.Sleep(50 * time.Millisecond)

	victims := det.CheckOnce(context.Background())
	if victims != nil {
		t.Errorf("no cycle should return nil, got: %v", victims)
	}
}

func TestDetector_RecentEdgeNotChecked(t *testing.T) {
	// 边创建时间 < threshold → 不计入图(避免短任务被误判)
	cfg := DetectorConfig{
		Threshold:     1 * time.Hour, // 很大
		CheckInterval: 1 * time.Second,
		DryRun:        false,
	}
	det := NewDetector(cfg, NewMockTrustGetter(), nil)

	det.RecordEdge("A", "B")
	det.RecordEdge("B", "A")

	// 立即检测,边还没到 threshold
	victims := det.CheckOnce(context.Background())
	if victims != nil {
		t.Errorf("recent edge should not trigger deadlock, got: %v", victims)
	}
}

func TestDetector_RemoveEdge(t *testing.T) {
	det := NewDetector(DefaultDetectorConfig(), NewMockTrustGetter(), nil)
	det.RecordEdge("A", "B")

	if det.EdgeCount() != 1 {
		t.Errorf("EdgeCount = %d, want 1", det.EdgeCount())
	}

	det.RemoveEdge("A", "B")
	if det.EdgeCount() != 0 {
		t.Errorf("EdgeCount after remove = %d, want 0", det.EdgeCount())
	}
}

func TestDetector_RemoveNode(t *testing.T) {
	det := NewDetector(DefaultDetectorConfig(), NewMockTrustGetter(), nil)
	det.RecordEdge("A", "B")
	det.RecordEdge("B", "A")
	det.RecordEdge("C", "A")

	if det.EdgeCount() != 3 {
		t.Errorf("EdgeCount = %d, want 3", det.EdgeCount())
	}

	det.RemoveNode("A")
	if det.EdgeCount() != 0 {
		t.Errorf("EdgeCount after remove A = %d, want 0", det.EdgeCount())
	}
}

func TestBreaker_DryRun_NoCallback(t *testing.T) {
	mock := NewMockTrustGetter()
	det := NewDetector(DefaultDetectorConfig(), mock, nil)
	cfg := BreakerConfig{DryRun: true, MaxKillPerHour: 10}
	br := NewBreaker(cfg, det, nil)

	called := false
	br.CancelCallback = func(ctx context.Context, victim string) error {
		called = true
		return nil
	}

	// dry-run 模式不会触发 callback
	br.Handle(context.Background(), []string{"A", "B"})
	if called {
		t.Error("dry-run should not trigger CancelCallback")
	}
}

func TestBreaker_RealKill_TriggersCallback(t *testing.T) {
	mock := NewMockTrustGetter()
	cfg := DetectorConfig{DryRun: false, Threshold: 10 * time.Millisecond}
	det := NewDetector(cfg, mock, nil)

	brCfg := BreakerConfig{DryRun: false, MaxKillPerHour: 10}
	br := NewBreaker(brCfg, det, nil)

	killed := []string{}
	br.CancelCallback = func(ctx context.Context, victim string) error {
		killed = append(killed, victim)
		return nil
	}

	br.Handle(context.Background(), []string{"A", "B"})

	if len(killed) != 2 {
		t.Errorf("expected 2 killed, got %d: %v", len(killed), killed)
	}
}

func TestBreaker_KillQuota(t *testing.T) {
	brCfg := BreakerConfig{DryRun: false, MaxKillPerHour: 3}
	br := NewBreaker(brCfg, nil, nil)

	called := 0
	br.CancelCallback = func(ctx context.Context, victim string) error {
		called++
		return nil
	}

	// 杀 5 次,第 4 次开始被 quota 限
	for i := 0; i < 5; i++ {
		br.Handle(context.Background(), []string{"v"})
	}

	if called != 3 {
		t.Errorf("expected 3 kills (quota), got %d", called)
	}
	if br.KillsInLastHour() != 3 {
		t.Errorf("KillsInLastHour = %d, want 3", br.KillsInLastHour())
	}
}

func TestBreaker_HandleEmpty(t *testing.T) {
	br := NewBreaker(DefaultBreakerConfig(), nil, nil)
	if br.Handle(context.Background(), nil) {
		t.Error("empty victims should return false")
	}
	if br.Handle(context.Background(), []string{}) {
		t.Error("empty slice should return false")
	}
}

func TestDefaultDetectorConfig(t *testing.T) {
	cfg := DefaultDetectorConfig()
	if cfg.Threshold != 5*time.Second {
		t.Errorf("Threshold = %v, want 5s", cfg.Threshold)
	}
	if !cfg.DryRun {
		t.Error("DryRun should default to true (v0.7.1)")
	}
}

func TestMockTrustGetter_DefaultScore(t *testing.T) {
	mock := NewMockTrustGetter()
	score, _ := mock.GetTrustScore(context.Background(), "unknown")
	if score != 0.5 {
		t.Errorf("default trust score = %f, want 0.5", score)
	}
}

func TestMockTrustGetter_SetScore(t *testing.T) {
	mock := NewMockTrustGetter()
	mock.SetScore("agent1", 0.8)
	score, _ := mock.GetTrustScore(context.Background(), "agent1")
	if score != 0.8 {
		t.Errorf("score = %f, want 0.8", score)
	}
}
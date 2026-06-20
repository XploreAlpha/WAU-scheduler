package deadlock

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// DetectorConfig 控制检测器行为
type DetectorConfig struct {
	// Threshold 任务等待超过这个时间才算"可能死锁"(per H6 默认 5s)
	Threshold time.Duration
	// CheckInterval 周期检测间隔(默认 1s)
	CheckInterval time.Duration
	// DryRun true 时只告警不杀(v0.7.1 默认),false 时真杀(v0.8.0)
	DryRun bool
}

// DefaultDetectorConfig 返回默认配置
//
// per A2 §2.2 锁定:
// - threshold=5s(避免误杀短任务)
// - checkInterval=1s(每秒扫一次)
// - dryRun=true(v0.7.1 默认,降低误杀;v0.8.0 切 false)
func DefaultDetectorConfig() DetectorConfig {
	return DetectorConfig{
		Threshold:     5 * time.Second,
		CheckInterval: 1 * time.Second,
		DryRun:        true,
	}
}

// Edge 1 条等待边 + 元数据
type Edge struct {
	From      string    // task_id 等待者
	To        string    // task_id 被等待者
	CreatedAt time.Time // 边创建时间
}

// TrustGetter 抽象 trust score 查询接口(避免循环依赖 wau-trust)
//
// 实现:
// - wau-trust HTTP client(wau-scheduler 生产用)
// - mock(测试用)
type TrustGetter interface {
	GetTrustScore(ctx context.Context, agentName string) (float64, error)
}

// Detector 死锁检测器
//
// 主循环:每 CheckInterval 跑一次
// 1. 找出所有 CreatedAt 早于 threshold 的边(老边)
// 2. 用这些边构建临时 WaitForGraph
// 3. Kahn 算法检测环
// 4. 有环 → 选 trust 最弱的节点杀掉(真杀)或告警(dry-run)
type Detector struct {
	cfg      DetectorConfig
	trust    TrustGetter
	logger   *slog.Logger

	mu         sync.RWMutex
	edges      map[string]Edge // edge_key ("from→to") → Edge

	stopCh chan struct{}
}

// NewDetector 创建检测器
func NewDetector(cfg DetectorConfig, trust TrustGetter, logger *slog.Logger) *Detector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Detector{
		cfg:    cfg,
		trust:  trust,
		logger: logger,
		edges:  make(map[string]Edge),
		stopCh: make(chan struct{}),
	}
}

// RecordEdge 记录 1 条新的等待边
//
// task 从 "等待" 状态进入时调用
func (d *Detector) RecordEdge(from, to string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := edgeKey(from, to)
	d.edges[key] = Edge{
		From:      from,
		To:        to,
		CreatedAt: time.Now(),
	}
}

// RemoveEdge 移除 1 条边
//
// task 完成时调用
func (d *Detector) RemoveEdge(from, to string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.edges, edgeKey(from, to))
}

// RemoveNode 移除节点相关的所有边
func (d *Detector) RemoveNode(node string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, e := range d.edges {
		if e.From == node || e.To == node {
			delete(d.edges, key)
		}
	}
}

// Start 启动后台检测循环
func (d *Detector) Start(ctx context.Context) {
	go d.loop(ctx)
}

// Stop 停止检测循环
func (d *Detector) Stop() {
	close(d.stopCh)
}

// loop 主循环
func (d *Detector) loop(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.checkOnce(ctx)
		}
	}
}

// CheckOnce 立即跑 1 次检测(导出用于测试 + e2e)
//
// 返回:被杀的节点(dry-run 时返 nil)
func (d *Detector) CheckOnce(ctx context.Context) []string {
	return d.checkOnce(ctx)
}

// checkOnce 内部检测逻辑
func (d *Detector) checkOnce(ctx context.Context) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 1. 找出所有"老边"(CreatedAt 早于 threshold)
	cutoff := time.Now().Add(-d.cfg.Threshold)
	graph := NewWaitForGraph()
	for _, e := range d.edges {
		if e.CreatedAt.Before(cutoff) {
			graph.AddEdge(e.From, e.To)
		}
	}

	// 2. Kahn 检测环
	hasCycle, deadlocked := graph.HasCycles()
	if !hasCycle {
		return nil
	}

	// 3. 有环!告警
	d.logger.Warn("deadlock detected",
		"nodes", deadlocked,
		"edge_count", graph.EdgeCount(),
		"threshold_seconds", d.cfg.Threshold.Seconds(),
		"dry_run", d.cfg.DryRun,
		"graph", graph.Snapshot(),
	)

	// 4. 干死锁:dry-run 只告警,真杀选 trust 最弱
	if d.cfg.DryRun {
		d.logger.Info("dry-run mode: would kill weakest trust node",
			"would_kill_candidates", deadlocked,
		)
		return nil
	}

	// 真杀模式(v0.8.0 启用):选 trust 最弱
	victim := d.weakestTrust(ctx, deadlocked)
	if victim == "" {
		return nil
	}

	d.logger.Error("deadlock breaker: killing weakest trust node",
		"victim", victim,
		"deadlocked_nodes", deadlocked,
	)

	// 注:实际"杀"逻辑由调用方实现 — 本 Detector 只返 victim,
	//   让 scheduler 触发 cancel + retry
	return []string{victim}
}

// weakestTrust 从 deadlocked 节点中选 trust 最弱的
//
// 1. 查 trust score
// 2. 选最小
// 3. 并列选字典序最小的(确定性)
func (d *Detector) weakestTrust(ctx context.Context, nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}

	minScore := 2.0 // > MaxTrustScore
	weakest := ""

	for _, n := range nodes {
		score, err := d.trust.GetTrustScore(ctx, n)
		if err != nil {
			d.logger.Warn("trust lookup failed for deadlock victim candidate",
				"agent", n, "error", err)
			// 出错时假设 0.5(默认 trust score)
			score = 0.5
		}

		if score < minScore || (score == minScore && n < weakest) {
			minScore = score
			weakest = n
		}
	}

	return weakest
}

// EdgeCount 返回当前活跃边数(用于监控)
func (d *Detector) EdgeCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.edges)
}

func edgeKey(from, to string) string {
	return from + "→" + to
}
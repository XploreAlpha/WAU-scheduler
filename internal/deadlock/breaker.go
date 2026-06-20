package deadlock

import (
	"context"
	"log/slog"
	"time"
)

// BreakerConfig 控制 breaker 行为
type BreakerConfig struct {
	// DryRun true 时只告警不杀(v0.7.1 默认),false 时真杀(v0.8.0)
	DryRun bool
	// AutoRecoverPeriod 检测到死锁后,等多久自动重新检测(默认 10s)
	AutoRecoverPeriod time.Duration
	// MaxKillPerHour 每小时最多杀节点数(防止误杀雪崩,默认 10)
	MaxKillPerHour int
}

// DefaultBreakerConfig 返回默认配置
//
// per A2 §2.2 + H6 §五 锁定:
// - DryRun=true(v0.7.1 默认开)
// - AutoRecoverPeriod=10s
// - MaxKillPerHour=10(防误杀雪崩)
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		DryRun:            true,
		AutoRecoverPeriod: 10 * time.Second,
		MaxKillPerHour:    10,
	}
}

// Breaker 把 Detector 跟调度器集成
//
// 1. 收到 detector 报的 victim
// 2. 检查每小时杀节点数(< MaxKillPerHour 才真杀)
// 3. 真杀模式:触发 CancelCallback
// 4. dry-run 模式:只 log
type Breaker struct {
	cfg      BreakerConfig
	detector *Detector
	logger   *slog.Logger

	// CancelCallback 真杀模式触发(victim task_id → cancel)
	// 调度器实现:停止 task + 入 dead-letter queue + 重试下一个
	CancelCallback func(ctx context.Context, victimTaskID string) error

	killTimestamps []time.Time
}

// NewBreaker 创建 breaker
func NewBreaker(cfg BreakerConfig, detector *Detector, logger *slog.Logger) *Breaker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Breaker{
		cfg:      cfg,
		detector: detector,
		logger:   logger,
	}
}

// Handle 处理器:detector 报 victim 后调用
//
// 返回:实际是否触发 CancelCallback
func (b *Breaker) Handle(ctx context.Context, victims []string) bool {
	if len(victims) == 0 {
		return false
	}

	// dry-run 模式不真杀
	if b.cfg.DryRun {
		b.logger.Info("breaker: dry-run, skipping actual kill",
			"victims", victims,
		)
		return false
	}

	// 真杀模式:检查 quota
	if !b.canKill() {
		b.logger.Warn("breaker: kill quota exhausted, skipping",
			"victims", victims,
			"max_per_hour", b.cfg.MaxKillPerHour,
		)
		return false
	}

	// 真杀每个 victim
	killed := 0
	for _, v := range victims {
		if b.CancelCallback == nil {
			b.logger.Warn("breaker: CancelCallback not set, cannot kill",
				"victim", v,
			)
			continue
		}

		if err := b.CancelCallback(ctx, v); err != nil {
			b.logger.Error("breaker: CancelCallback failed",
				"victim", v, "error", err,
			)
			continue
		}

		b.recordKill()
		// 同时清 detector 的边
		if b.detector != nil {
			b.detector.RemoveNode(v)
		}
		killed++

		b.logger.Info("breaker: killed victim",
			"victim", v,
			"dry_run", false,
		)
	}

	return killed > 0
}

// canKill 检查 1 小时内是否还有 quota 可杀
func (b *Breaker) canKill() bool {
	cutoff := time.Now().Add(-1 * time.Hour)

	// 清掉 1 小时前的记录
	filtered := b.killTimestamps[:0]
	for _, ts := range b.killTimestamps {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}
	b.killTimestamps = filtered

	return len(b.killTimestamps) < b.cfg.MaxKillPerHour
}

// recordKill 记录 1 次杀
func (b *Breaker) recordKill() {
	b.killTimestamps = append(b.killTimestamps, time.Now())
}

// KillsInLastHour 返回过去 1 小时杀节点数(用于监控)
func (b *Breaker) KillsInLastHour() int {
	cutoff := time.Now().Add(-1 * time.Hour)
	n := 0
	for _, ts := range b.killTimestamps {
		if ts.After(cutoff) {
			n++
		}
	}
	return n
}

// MockTrustGetter 测试用 mock
type MockTrustGetter struct {
	Scores map[string]float64
}

// NewMockTrustGetter 创建 mock
func NewMockTrustGetter() *MockTrustGetter {
	return &MockTrustGetter{
		Scores: make(map[string]float64),
	}
}

// GetTrustScore 实现 TrustGetter 接口
func (m *MockTrustGetter) GetTrustScore(ctx context.Context, agentName string) (float64, error) {
	if s, ok := m.Scores[agentName]; ok {
		return s, nil
	}
	return 0.5, nil // 默认 trust score
}

// SetScore 设置 1 个 agent 的 trust score
func (m *MockTrustGetter) SetScore(agent string, score float64) {
	m.Scores[agent] = score
}
package scheduler

import (
	"time"
)

// Task 任务结构
type Task struct {
	TaskID        string
	Status        string // pending, dispatched, running, completed, failed
	Intent        Intent
	AssignedAgent string
	RetryCount    int
	CreatedAt     int64
	UpdatedAt     int64
}

// Intent 任务意图
type Intent struct {
	Type             string   // task, query, file, payment
	RequiredSkills   []string // 所需技能列表
	Urgency          string   // low, normal, high
	EstimatedTimeout int      // 预估超时时间（秒）
	SourceUniverse   string   // 来源 Universe
}

// ScheduleRequest 调度请求
type ScheduleRequest struct {
	Task            *Task
	RequiredSkills  []string
	IntentType      string
	Urgency         string
	SourceUniverse  string
	MaxRetry        int // 最大重试次数，默认3
}

// ScheduleResult 调度结果
type ScheduleResult struct {
	Task      *Task
	AgentID   string
	Score     float64
	DispatchedAt time.Time
}

// Weights 15维度权重配置
type Weights struct {
	SkillMatch     float64 // 0.25
	TrustScore     float64 // 0.20
	HealthScore    float64 // 0.15
	LatencyScore   float64 // 0.10
	LoadScore      float64 // 0.08
	SuccessRate    float64 // 0.07
	NetworkPenalty float64 // 0.05
	BandwidthScore float64 // 0.03
	AuthLevel      float64 // 0.02
	ProtocolCompat float64 // 0.02
	HistoryCount   float64 // 0.01
	ErrorRate      float64 // 0.01
	Availability   float64 // 0.005
	VersionCompat  float64 // 0.005
	GeoPenalty     float64 // 0.003 (v0.7.0 修复:让总和 = 1.000)
}

// DefaultWeights 返回默认权重
func DefaultWeights() Weights {
	return Weights{
		SkillMatch:     0.25,
		TrustScore:     0.20,
		HealthScore:    0.15,
		LatencyScore:   0.10,
		LoadScore:      0.08,
		SuccessRate:    0.07,
		NetworkPenalty: 0.05,
		BandwidthScore: 0.03,
		AuthLevel:      0.02,
		ProtocolCompat: 0.02,
		HistoryCount:   0.01,
		ErrorRate:      0.01,
		Availability:   0.005,
		VersionCompat:  0.005,
		GeoPenalty:     0.0, // v0.7.0 修复:让总和 = 1.000(W2 接 AgentMeta.Region 后再启用)
	}
}

// AgentScore Agent评分结果
type AgentScore struct {
	AgentID     string
	TotalScore  float64
	Dimensions  DimensionScores
	Rank        int
}

// DimensionScores 各维度分数
type DimensionScores struct {
	SkillMatch      float64 // 0-1
	TrustScore      float64 // 0-1
	HealthScore     float64 // 0-1
	LatencyScore    float64 // 0-1 (延迟越低越高)
	LoadScore       float64 // 0-1 (负载越低越高)
	SuccessRate     float64 // 0-1
	NetworkPenalty  float64 // 0.7-1.0
	BandwidthScore  float64 // 0-1
	AuthLevel       float64 // 0-1
	ProtocolCompat  float64 // 0-1
	HistoryCount    float64 // 0-1
	ErrorRate       float64 // 0-1
	Availability    float64 // 0-1
	VersionCompat   float64 // 0-1
	GeoPenalty      float64 // 0.9-1.0
}

const (
	// TaskStatus 任务状态
	TaskStatusPending    = "pending"
	TaskStatusDispatched = "dispatched"
	TaskStatusRunning    = "running"
	TaskStatusCompleted  = "completed"
	TaskStatusFailed     = "failed"

	// DefaultMaxRetry 默认最大重试次数
	DefaultMaxRetry = 3

	// DefaultTimeout 默认任务超时时间
	DefaultTimeout = 5 * time.Minute
)

const (
	// WatchdogInterval Watchdog检查间隔
	WatchdogInterval = 30 * time.Second

	// DLQCapacity DLQ最大容量
	DLQCapacity = 1000
)

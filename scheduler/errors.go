package scheduler

// SchedulerError 调度器错误
type SchedulerError struct {
	Code    string
	Message string
}

func (e *SchedulerError) Error() string {
	return e.Message
}

// NewSchedulerError 创建调度器错误
func NewSchedulerError(code, message string) *SchedulerError {
	return &SchedulerError{Code: code, Message: message}
}

var (
	// ErrNoAvailableAgent 没有可用Agent
	ErrNoAvailableAgent = NewSchedulerError("NO_AGENT", "No available agent found")

	// ErrTaskNotFound 任务不存在
	ErrTaskNotFound = NewSchedulerError("TASK_NOT_FOUND", "Task not found")

	// ErrMaxRetries 超过最大重试次数
	ErrMaxRetries = NewSchedulerError("MAX_RETRIES", "Max retries exceeded")

	// ErrInvalidTask 无效任务
	ErrInvalidTask = NewSchedulerError("INVALID_TASK", "Invalid task")

	// ErrSchedulerClosed 调度器已关闭
	ErrSchedulerClosed = NewSchedulerError("SCHEDULER_CLOSED", "Scheduler is closed")

	// v0.8.0 M4-3.2: replication errors (self-replication policy gates)

	// ErrReplicateNotImplemented — DataSource does not implement Replicate.
	// Returned by MemoryDataSource and RedisDataSource. Production callers
	// must wrap with WauTrustDataSource (which delegates to wau-trust Engine).
	ErrReplicateNotImplemented = NewSchedulerError("REPLICATE_NOT_IMPLEMENTED",
		"DataSource does not implement Replicate (use WauTrustDataSource)")

	// ErrPolicyDisabled — no replication policy set on Scheduler.
	// Mirrors the nil-policy semantics of ColdRoutingPolicy / SleepPolicy.
	// Backward compat: pass nil to NewSchedulerWithReplicationPolicy to disable.
	ErrPolicyDisabled = NewSchedulerError("POLICY_DISABLED",
		"no replication policy set on scheduler")

	// ErrParentNotFound — parent agent not in registry.
	// Returned by Scheduler.Replicate before any trust / policy check.
	ErrParentNotFound = NewSchedulerError("PARENT_NOT_FOUND",
		"parent agent not in registry")

	// ErrParentCold — parent has no trust data; warm up via cold routing first.
	// Returned by Scheduler.Replicate after registry lookup but before trust read.
	// Distinguished from ErrParentLowTrust: cold = no data; low trust = has data
	// but score below threshold.
	ErrParentCold = NewSchedulerError("PARENT_COLD",
		"parent has no trust data, warm up before replicate")

	// ErrParentLowTrust — parent trust below policy minimum (default 0.8).
	// Returned by ReplicationPolicy.CanReplicate when parentTrust < MinParentTrust.
	ErrParentLowTrust = NewSchedulerError("PARENT_LOW_TRUST",
		"parent trust below policy minimum")

	// ErrChildLimitReached — parent already at MaxChildrenPerParent children.
	// Returned by ReplicationPolicy.CanReplicate when currentChildren ≥ limit.
	ErrChildLimitReached = NewSchedulerError("CHILD_LIMIT_REACHED",
		"parent already at max children count")

	// ErrInvalidChildID — child ID is empty or otherwise invalid.
	// Returned by Scheduler.Replicate early-exit before any registry lookup.
	ErrInvalidChildID = NewSchedulerError("INVALID_CHILD_ID",
		"child ID is empty or invalid")
)

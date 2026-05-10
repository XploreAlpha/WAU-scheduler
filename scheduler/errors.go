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
)

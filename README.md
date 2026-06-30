# wau-scheduler

> WAU 网络的任务调度器模块 - 基于评分的智能任务调度

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat-square&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

---

## 核心设计

**基于评分的智能调度** - 通过 15 维度评分选择最合适的 Agent 执行任务。

```
传统调度：随机选择 / 轮询 / 基于规则
          ↓
wau-scheduler：多维度评分 → 选择最优 Agent
              ↑
         Registry 获取在线 Agent
```

---

## 调度流程

```
Task 提交
    │
    │ 从 Registry 获取在线 Agent 列表
    │
    ▼
15 维度评分
    ├── SkillMatch (0.25) - 技能匹配度
    ├── TrustScore (0.20) - 信任分数
    ├── HealthScore (0.15) - 健康状态
    ├── LatencyScore (0.10) - 延迟分数
    ├── LoadScore (0.08) - 负载分数
    └── ... (其他维度)
            │
            ▼
    选择评分最高的 Agent
            │
            ▼
    返回调度结果 (AgentID + Score)
```

---

## 15 维度评分体系

| # | 维度 | 权重 | 说明 |
|---|------|------|------|
| 1 | SkillMatch | 0.25 | 技能匹配度 |
| 2 | TrustScore | 0.20 | 信任分数 |
| 3 | HealthScore | 0.15 | 健康状态 (CPU/内存) |
| 4 | LatencyScore | 0.10 | 延迟分数 |
| 5 | LoadScore | 0.08 | 负载分数 |
| 6 | SuccessRate | 0.07 | 成功率 |
| 7 | NetworkPenalty | 0.05 | 网络惩罚 (跨 Universe) |
| 8 | BandwidthScore | 0.03 | 带宽可用性 |
| 9 | AuthLevel | 0.02 | 认证级别 |
| 10 | ProtocolCompat | 0.02 | 协议兼容性 |
| 11 | HistoryCount | 0.01 | 历史交互次数 |
| 12 | ErrorRate | 0.01 | 错误率 |
| 13 | Availability | 0.005 | 可用性 |
| 14 | VersionCompat | 0.005 | 版本兼容性 |
| 15 | GeoPenalty | 0.005 | 地理位置惩罚 |

---

## 接口设计

```go
// Scheduler 调度器
type Scheduler struct{}

// Schedule 调度任务
func (s *Scheduler) Schedule(ctx context.Context, req *ScheduleRequest) (*ScheduleResult, error)

// ScheduleSimple 简化调度 - 输入技能列表，返回最佳 Agent
func (s *Scheduler) ScheduleSimple(ctx context.Context, requiredSkills []string) (*AgentScore, error)

// StartWatchdog 启动 Watchdog
func (s *Scheduler) StartWatchdog(ctx context.Context)

// StopWatchdog 停止 Watchdog
func (s *Scheduler) StopWatchdog()
```

---

## 数据结构

### ScheduleRequest

```go
type ScheduleRequest struct {
    Task            *Task      // 任务信息
    RequiredSkills  []string   // 所需技能
    IntentType     string     // 意图类型
    Urgency         string    // 紧急度
    SourceUniverse  string    // 来源 Universe
    MaxRetry        int       // 最大重试次数
}
```

### ScheduleResult

```go
type ScheduleResult struct {
    Task         *Task
    AgentID      string
    Score        float64
    DispatchedAt time.Time
}
```

### AgentScore

```go
type AgentScore struct {
    AgentID     string
    TotalScore  float64
    Dimensions  DimensionScores
    Rank        int
}
```

---

## 与 wau-registry 的关系

```
wau-scheduler 依赖 wau-registry 获取在线 Agent 信息：

wau-scheduler
    │
    └── wau-registry (独立模块)
            │
            ├── GetOnlineAgents() 获取在线 Agent
            ├── GetAgent() 获取单个 Agent 信息
            └── GetLoad() 获取负载信息

依赖: wau-scheduler → github.com/wau/registry
```

---

## 项目结构

```
wau-scheduler/
├── scheduler/
│   ├── types.go    # 类型定义
│   ├── errors.go   # 错误定义
│   ├── scoring.go  # 15维度评分引擎
│   └── scheduler.go # 调度器实现
├── go.mod
└── README.md
```

---

## v0.9.0 "Acorn" 收口段(2026-09-15 GA)

上文详细介绍了 v0.3.0-v0.8.0 实装历史 + 测试 + 编译段。本段为 v0.9.0 GA 增量补充。

### 角色

| OS 类比 | Process Scheduler(进程调度器)|
|---|---|
| 部署 | 独立 git 仓 = `wau-scheduler`,WAU-core-kernel 6 子模块之一 |
| 通信 | gRPC 接收 WAU-core-kernel 调度请求 / 与其他 5 子模块协作 |
| 状态 | v0.8.0 GA 已发(2026-07-13)|

### v0.9.0 集成

- **接 WAU-core-kernel**:Task 队列化 + Worker 池 + 优先级
- **降级方案**:W-6 调度失败 → 走 fallback 队列 → 仍可执行
- **3 新仓接入**:wau-edge / wau-llm-router / wau-channel 都通过 WAU-core-kernel 间接调度到本仓
- **不破 wire**:Task 提交协议(per WAU-core-kernel/proto/task.proto)100% 保留

### v0.9.0 "Acorn" 5 份核心文档

| # | 文件 | 内容 |
|---|---|---|
| 1 | [README.md](README.md)(本文件)| 仓入口 + 实装历史 + v0.9.0 收口段 |
| 2 | [QUICKSTART.md](QUICKSTART.md) | 15 分钟跑通最小场景 |
| 3 | [DEPLOY.md](DEPLOY.md) | 部署 + Worker 池配置 |
| 4 | [ARCHITECTURE.md](ARCHITECTURE.md) | 模块拆分 + 数据流 |
| 5 | [CHANGELOG.md](CHANGELOG.md) | v0.8.0 + v0.9.0 倒序 |

### 历史锚点

- v0.8.0 GA([[project-v0.8.0-GA-2026-07-13]])
- v0.9.0 主 plan([[project-v0-9-0-development-plan-2026-06-27]])

## 编译验证

```bash
cd /home/inamoto888/project/wau-scheduler
go build ./...
```

---

## License

MIT License - 详见 [LICENSE](LICENSE) 文件

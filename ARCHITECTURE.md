# wau-scheduler 架构

## 模块拆分

```
wau-scheduler/
├── cmd/wau-scheduler/main.go     # 主入口
├── internal/
│   ├── config/                   # YAML 配置
│   ├── queue/                    # Task 队列(Priority Queue)
│   ├── worker/                   # Worker Pool(N goroutine)
│   ├── scheduler/                # 调度核心(Submit / Pick / Complete)
│   └── metrics/                  # prom 指标占位
├── proto/                        # gRPC 接口
├── configs/scheduler.yaml
├── tests/
└── README.md / QUICKSTART.md / DEPLOY.md / ARCHITECTURE.md / CHANGELOG.md
```

## 数据流

```
WAU-core-kernel (gRPC SubmitTask)
    ↓
priority_queue.Push(Task{ID, Priority, Payload})
    ↓
worker_pool[].Pick() → 阻塞等待
    ↓
Worker.Run(Task)
    ↓ 完成后
Worker.Complete(TaskID) → release
```

## 关键决策

| 决策 | 内容 |
|---|---|
| **6 子模块之一** | per [[project-wau-core-product-list-2026-06-28]] |
| **独立 git 仓** | 2026-06-28 起跟 WAU-core-kernel 拆分子目录独立发版 |
| **不破 wire** | Task 协议 100% 兼容老客户端 |

## 接口边界

- **入**:gRPC SubmitTask(从 WAU-core-kernel)
- **出**:Worker.Run 完成后 gRPC Notify 回 Kernel
- **依赖**:无强外部依赖
- **被依赖**:WAU-core-kernel(主)+ wau-edge / wau-channel / wau-llm-router(间接)

## 性能预算

| 指标 | 目标 |
|---|---|
| SubmitTask P50 | < 1 ms |
| Worker Pick 延迟 | < 5 ms |
| 队列容量 | 10k Task |
| 并发 Worker | N (configurable,默认 4) |

## 跟其他仓的关系

- **上游(调用本仓)**:WAU-core-kernel
- **下游**:无
- **同组(WAU-core-kernel 6 子模块)**:wau-trust / wau-circuit / wau-profile / wau-intent / wau-registry / wau-registry-service

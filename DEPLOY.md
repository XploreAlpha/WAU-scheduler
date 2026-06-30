# wau-scheduler 部署

## 端口

| 端口 | 类型 | 端点 |
|---|---|---|
| 18450 | gRPC | `wau.scheduler.v1.Scheduler` service |
| 18451 | HTTP | `/healthz` + `/metrics`(如有)|

## 部署模式

wau-scheduler 跟 WAU-core-kernel 同物理机部署或独立 Worker 集群部署均可:

```
模式 A: 同机部署(开发)
WAU-core-kernel + wau-scheduler(同进程 / 不同进程)

模式 B: Worker 集群(生产)
WAU-core-kernel + N × wau-scheduler Worker(通过 gRPC 注册中心拉任务)
```

## 监控

```bash
curl -s http://localhost:18451/metrics | grep wau_scheduler
```

待 §2.11 拍板后定(per [[project-wau-core-product-list-2026-06-28]] 同步 wau_channel_adapter_* 风格)。

## 进程管理

```bash
tmux new -d -s wau-scheduler '/tmp/wau-scheduler -config ~/.wau/scheduler.yaml'
```

## 配置

| 字段 | 默认 | 说明 |
|---|---|---|
| `grpc.addr` | `:18450` | gRPC 监听 |
| `worker_pool.size` | `4` | 并发 Worker 数 |
| `queue.max_size` | `10000` | Task 队列容量上限 |
| `fallback.timeout_ms` | `5000` | 5 秒后 fallback |

## 升级路径

- v0.9.0(Acorn)→ v0.8.0(Sprout):
  - Task 协议 100% 兼容
  - 老 Queue / Worker 配置自动迁移
- v0.9.0 → v1.0.0:多租户 tier 隔离

# wau-scheduler 15 分钟跑通

> 目标:本机启 wau-scheduler + 1 个 Task,验证调度器能入队 + 1 个 Worker 处理。

## 前置

- Go 1.21+
- 端口 :18450(gRPC)/ :18451(HTTP,如启用):本机空闲
- OS:Linux / macOS / WSL2

## 步骤

### 1. 拉源码

```bash
cd ~/project/wau-scheduler
git pull origin main
make build
ls bin/
```

### 2. 复制最小配置

```bash
mkdir -p ~/.wau
cp configs/scheduler.yaml ~/.wau/
```

### 3. 启

```bash
./bin/wau-scheduler -config ~/.wau/scheduler.yaml
# 预期:[wau-scheduler] gRPC server starting on :18450
```

### 4. 提交 1 个测试 Task

```bash
# 方式 A: WAU-core-kernel 间接转
cd ~/project/WAU-core-kernel && ./bin/wau-core-kernel -config ~/.wau/kernel.yaml

# 方式 B: 直接 gRPC client(grpcurl)
grpcurl -plaintext -d '{"task_id":"test-1","priority":1}' \
  127.0.0.1:18450 wau.scheduler.v1.Scheduler/Submit
```

## 下一步

- [DEPLOY.md](DEPLOY.md) — Worker 池配置 + 队列策略
- [ARCHITECTURE.md](ARCHITECTURE.md) — 内部模块 + 数据流
- [README.md](README.md) — v0.9.0 收口段

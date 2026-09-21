# task-queue-engine (Python)

本地可运行的任务队列与事件消费子系统：SQLite(WAL) 持久化 + 线程池执行，
覆盖有界队列、消费者组位点、幂等去重、退避重试/死信、故障恢复、负载演进、
错误归一化与可度量性。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --port 8000     # 启动 HTTP 服务（默认 taskqueue.db）
uv run pytest -q                         # 运行全部测试
```

提交任务（处理器名放在 `payload.metadata.handler`）：

```bash
curl -X POST localhost:8000/v1/namespaces/shop/tasks \
  -H 'content-type: application/json' \
  -d '{"namespace":"shop","payload":{"args":[42],"metadata":{"handler":"h"}},
       "dedup_key":"order:42"}'
```

## 代码内使用

```python
from taskqueue.engine import TaskQueueEngine
from taskqueue.config import EngineConfig

with TaskQueueEngine(EngineConfig(db_path="tasks.db"),
                     handlers={"h": lambda p: print(p["args"])}) as eng:
    t = eng.submit("shop", {"args": [1], "metadata": {"handler": "h"}},
                   dedup_key="k1")
    eng.wait_idle(timeout=5)
    print(eng.get_task(t.id).status)
```

## 能力与语义

- **有界隔离**：namespace/queue 隔离，长度与积压字节双上限，超限 reject/delay。
- **位点**：组内连续位点原子提交；重复幂等、回退/越位/非租约者明确拒绝。
- **去重**：窗口内同 key 一次执行，过期边界确定、跨重启保留。
- **重试/死信**：确定性指数退避；不可重试或超上限入可检索死信。
- **恢复**：终态不重复、未决不丢失；重试预算与位点跨重启保留。
- **演进**：负载版本化，缺省稳定，非法记录明确拒绝。
- **错误**：统一错误类别，HTTP 状态码可区分，底层异常不透出。
- **度量**：`GET /v1/metrics` 给出吞吐与 p50/p95/p99 延迟。

详细设计见 [`docs/design.md`](docs/design.md)。

## 测试

测试以并发认领、重复提交、故障重启与错误分类为重点，单测日志打印
任务标识、位点与判定依据：

```bash
uv run pytest -q --log-cli-level=INFO
```

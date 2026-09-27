# task-queue-engine-python

本地可运行的任务队列与事件消费子系统：任务提交、异步执行、重试、
去重、死信与消费者组位点管理，在并发、重启与故障场景下给出一致且
可解释的结果。持久化基于 SQLite（WAL），无外部服务依赖。

## 运行

```bash
uv sync
uv run pytest            # 全部测试（39 个）
uv run python bench.py   # 吞吐/延迟基准
uv run uvicorn main:app  # HTTP 服务
```

## 队列模型

- **命名空间隔离**：任务按 `namespace` 划分，积压上限按命名空间独立
  统计，互不影响。
- **有界积压**：每个命名空间有任务数（`max_queue_size`）与字节数
  （`max_backlog_bytes`）上限。超限按 `overflow_policy` 处理：
  - `reject`（默认）：立即返回 `resource_exhausted` 错误；
  - `defer`：等待额度释放，最长 `defer_timeout_seconds`，超时仍拒绝。
- **持久化先行**：任务先落库再被调度，空闲时调度线程按
  `poll_interval_seconds` 休眠，不持有任务本体，资源占用有界。
- **执行模型**：调度线程批量原子认领到期任务（`claim_due` 在单事务内
  `SELECT+UPDATE`，不会重复认领），交由有界线程池执行。

## 位点与去重规则

### 消费位点（consumer group）

- 位点 = 下一条待消费事件的 offset，按 `(group, topic)` 持久化。
- **组内单活**（`ConsumerGroup`，租约模式）：同一 `(group, topic)`
  通过租约保证同一时刻只有一个活跃消费者，多实例并行不会重复认领；
  持有者退出或租约过期后由其他实例接管，从已提交位点继续。
- **原子提交**：事件处理成功后才推进位点；提交在单事务内完成。
  **重复提交相同位点或位点回退会被明确拒绝**（`conflict`），不会从
  中间继续。
- 处理失败时位点不前进，事件会被重投（at-least-once），处理器需幂等。

### 组内并行消费（`ParallelConsumer`）

- **真正并行**：同一消费组内可同时跑多个消费者实例，共同处理同一
  事件流。认领在 store 层单事务内完成：同一时刻同一条事件只被一个
  实例持有，组内各实例处理的事件不重叠。
- **在途上限**：组在途（已认领未确认）事件数受 `max_inflight`（默认
  64）约束，积压时不会无限认领；空闲时不持有任何事件，认领/确认
  记录随位点推进即时清理，资源占用有界。
- **位点连续推进**：确认（ack）乱序到达时先记入待决集合，组对外
  可见的完成位点只在中间无空缺时连续推进——后面的先完成不跳号，
  等缺口补齐后一次性越过，不丢事件。
- **重投与幂等确认**：处理失败立即释放认领（nack），实例崩溃时认领
  随 `claim_ttl` 过期，事件都会被组内其他实例接手重做，不会永久
  卡住。ack 幂等：位点已越过或已确认的重复确认不计数、不产生重复
  位点提交，同一条事件不会被算成两次成功。
- 消费者正常退出（`close()`）会释放其全部在途认领，事件立即可被
  他人接手，无需等待 TTL。

### 提交去重（dedup key）

- 提交时携带 `dedup_key`，窗口期（`dedup_window_seconds`，默认 1 小时）
  内相同键的重复提交返回**已有任务**，不产生重复执行或重复副作用。
- **队列满时同样生效**：去重判定先于积压容量检查——命名空间积压
  打满时，命中有效期内相同去重键的提交直接返回已有任务，而不是
  报 `resource_exhausted`，也不新增积压。
- 窗口语义确定：`expires_at = submit_time + dedup_window_seconds`，
  过期后同键可再次提交为新任务。去重判定与任务插入在同一事务内，
  并发下同键只会有一个任务被创建。

## 重试与死信

- 处理器抛 `RetryableError`（或未知异常）：按指数退避重试，
  `delay = backoff_base_seconds * 2^(attempts-1)`。
- 重试次数超过 `max_retries`，或处理器抛 `FatalError`：进入死信
  （`dead`），可通过 `GET /dead` 或 `engine.list_dead()` 检索。
- 任务超时（`task_timeout_seconds`）按可重试失败处理；取消仅对非终态
  任务生效，已终态任务的取消/重复完成会被明确拒绝，状态不会半更新。

## 崩溃恢复

- 进程重启后 `start()` 把残留的 `RUNNING` 任务重置为 `PENDING` 续跑；
  已 `SUCCEEDED` 的任务不会重复执行，未完成任务不丢失。
- 边界规则：副作用生效与状态落库之间崩溃时，任务会被重投——因此
  **处理器必须幂等**；跨重启的副作用去重由提交时的 `dedup_key` 保证。

## 负载结构演进

- 负载带版本号（当前 v2）。旧版本负载解码时自动迁移并补**稳定缺省**
  （如 v1 无 `priority`，迁移后补 `0`）。
- 比当前更新的版本：**明确拒绝**（`validation`），不静默尝试。
- 字段缺失（无缺省值）、类型不符、未知字段：明确拒绝，不静默丢弃。

## 错误模型

所有对外错误归一化为 `QueueError`，底层异常不透出：

| category | HTTP | 含义 |
|---|---|---|
| `validation` | 400 | 参数/负载结构错误 |
| `resource_exhausted` | 429 | 积压超限 |
| `conflict` | 409 | 状态冲突（位点回退、重复提交、终态操作） |
| `not_found` | 404 | 任务不存在 |
| `internal` | 500 | 内部异常（细节不透出） |

## 配置项（`EngineConfig`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `max_queue_size` | 1024 | 单命名空间最大积压任务数 |
| `max_backlog_bytes` | 4 MiB | 单命名空间最大积压字节数 |
| `overflow_policy` | `reject` | 超限策略：`reject` / `defer` |
| `defer_timeout_seconds` | 1.0 | `defer` 策略等待额度的上限 |
| `max_retries` | 3 | 最大重试次数 |
| `backoff_base_seconds` | 0.05 | 退避基数（指数退避） |
| `task_timeout_seconds` | 30.0 | 单次执行超时 |
| `dedup_window_seconds` | 3600 | 去重窗口有效期 |
| `workers` | 4 | 执行线程数 |
| `poll_interval_seconds` | 0.01 | 空闲轮询间隔 |
| `claim_batch` | 16 | 单次认领批量 |

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 提交任务（`namespace`/`payload`/`dedup_key`） |
| GET | `/tasks/{id}` | 查询任务状态 |
| POST | `/tasks/{id}/cancel` | 取消非终态任务 |
| GET | `/dead` | 检索死信 |
| POST | `/events` | 追加事件到 topic |
| POST | `/consumer-groups/{g}/poll` | 消费一批事件并推进位点 |
| GET | `/consumer-groups/{g}/sink` | 查看该组已消费事件 |
| GET | `/offsets` | 查询消费位点 |
| GET | `/metrics` | 吞吐/延迟度量 |
| GET | `/health` | 健康检查 |

## 度量

`GET /metrics` 返回计数器（submitted/succeeded/retried/dead/
dedup_hits/cancelled/recovered）、吞吐（成功任务/秒）与延迟分布
（avg/p50/p99/max，有界滑动窗口）。本地基准（`bench.py`，2000 个
空操作任务）：提交约 7.6k/s，端到端约 7.1k/s，p99 延迟约 2ms。

## 兼容范围与本地验证

- **兼容范围**：纯 Python 标准库 + FastAPI 运行时，Python 3.12+；
  持久化仅依赖 SQLite（WAL），无外部服务。`ParallelConsumer` 与
  既有 `ConsumerGroup` 租约模式并存、互不影响，既有 API 与位点
  语义保持不变；同一 `(group, topic)` 请只选用其中一种模式。
- **本地验证**：
  - `uv run pytest tests/test_parallel_consumer.py --log-cli-level=INFO`
    查看组内并行、崩溃重投、乱序连续推进、在途上限、满员去重的
    测试日志（含事件 offset、位点变化与判定依据）；
  - `uv run pytest` 跑全部 39 个测试确认无回归；
  - `uv run python bench.py` 复核吞吐/延迟基准。

## 目录结构

```
taskqueue/
  errors.py    # 归一化错误类别
  payload.py   # 负载版本化编解码与校验
  models.py    # Task/Event 数据模型
  store.py     # SQLite 持久化：任务、去重键、位点、事件日志、租约、
               # 并行认领/确认（event_claims/event_acks）
  queue.py     # 按命名空间的积压限制器（reject/defer）
  engine.py    # 任务引擎：提交、调度、重试、死信、恢复
  consumer.py  # 消费者组：租约单活模式 + 组内并行模式
  metrics.py   # 吞吐与延迟度量
main.py        # FastAPI HTTP 接口
bench.py       # 吞吐/延迟基准
tests/         # 并发、去重、故障重启、错误分类测试
```

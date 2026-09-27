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
- **组内并行消费**：同一 `(group, topic)` 可同时存在多个消费者
  （`ConsumerGroup` 实例，各有唯一 `owner`）真正并行处理同一事件流。
  每条事件在 `event_claims` 中对应一行状态，单事务 UPSERT 保证
  **同一时刻同一条事件只被一个消费者有效认领**，组内各消费者处理的
  事件互不重叠。
- **位点连续推进（不跳号）**：每条事件处理成功后 `ack`，组位点只
  连续推进到当前的“最大无缺口前缀”。乱序完成时，先完成的高 offset
  不会越过中间尚未确认的缺口；缺口事件被确认后，下一次 ack 把位点
  一次性推进到位。一轮结束后位点恰好停在所有事件之后，不跳号、不丢。
- **重投与去重**：处理失败立即放弃认领（`nack`），事件可被本组任意
  消费者（包括自己下一轮）重领；消费者崩溃（停止续租）后，认领在
  `lease_ttl` 到期时失效，由其他消费者接手未确认的事件。`ack` 幂等：
  重复确认、被接手后的迟到确认返回 `applied=False`，**同一条事件不会
  被算成两次成功，也不产生重复的位点提交**。手动 `commit` 仍保持
  “只许严格前进”的旧规则（重复/回退抛 `conflict`）。
- **在途上限有界**：`max_inflight`（默认 32）限定同一消费组的在途事件
  数（已认领未确认且未过期）。无论积压多长，组内认领总量不超过该上限；
  空闲时无在途事件，高压下在途事件与并发处理器数同样有界。HTTP 轮询
  接口可通过 `max_inflight` 查询参数配置。
- 长耗时事件在处理前自动续租认领，避免处理途中被误判崩溃而重投；
  语义仍是 at-least-once，**处理器需幂等**。

### 提交去重（dedup key）

- 提交时携带 `dedup_key`，窗口期（`dedup_window_seconds`，默认 1 小时）
  内相同键的重复提交返回**已有任务**，不产生重复执行或重复副作用。
- 窗口语义确定：`expires_at = submit_time + dedup_window_seconds`，
  过期后同键可再次提交为新任务。去重判定与任务插入在同一事务内，
  并发下同键只会有一个任务被创建。
- **队列打满时的去重优先**：去重判定先于容量判定。命名空间积压已经
  打满时，若提交命中的是仍在有效期内的相同去重键，直接返回之前那笔
  已有任务（`created=False`），**不占用积压额度、不报资源不足**；
  只有未命中去重（新键/键已过期）的提交才会因容量被拒绝
  （`resource_exhausted`）或按 `defer` 策略等待。

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

## 兼容范围

- `ConsumerGroup(store, group, topic, handler, *, owner, lease_ttl,
  metrics, clock)` 的原有参数与 `poll_once()`/`commit()`/`close()`/
  `committed_offset` 签名保持不变；新增可选关键字参数 `max_inflight`
  （默认 32）。旧代码无需改动即可从“组内单活”自动升级为“组内并行”。
- 事件日志（`events`）、位点表（`offsets`）、旧的组租约表（`leases`）
  结构与语义保留；并行认领使用新增的 `event_claims` 表，旧库打开时
  通过 `CREATE TABLE IF NOT EXISTS` 自动补建，无需手工迁移。
- 手动 `commit(offset)` 仍是“只许严格前进”；`store.commit_offset`
  的重复/回退拒绝规则不变。
- 任务侧（提交/执行/重试/死信）语义不变；唯一行为加强：**队列打满时
  有效期内的重复去重键不再返回容量错误**，而是返回已有任务。
- 持久化仍是单文件 SQLite（WAL），同一数据库文件支持多线程、单进程
  内并行；不支持跨进程同时写同一文件。

## 本地验证

```bash
uv sync
uv run pytest -q                      # 39 个用例全部通过
uv run pytest tests/test_consumer.py  # 并行消费/乱序位点/崩溃重投/在途上限
uv run pytest tests/test_engine.py    # 含队列打满时重复去重键用例
uv run python -c "import main; main.create_app()"   # HTTP 应用可加载
uv run python bench.py                # 基准回归对比
```

关键新行为对应测试：

| 场景 | 用例 |
|---|---|
| 同组 6 消费者并发处理 1000 条事件，不重不丢、位点连续 | `test_multi_consumers_parallel_large_batch` |
| 消费者崩溃后未确认事件被接手重投，迟到 ack 不生效 | `test_crashed_consumer_events_redelivered` |
| 乱序完成时位点不越过缺口、补齐后连续推进 | `test_out_of_order_commits_stay_contiguous` |
| 在途数量达到 `max_inflight` 后不再认领（有界） | `test_inflight_cap_bounded` |
| 失败重投不产生重复位点提交/重复成功 | `test_redelivery_no_double_offset_commit` |
| 队列打满时有效期内相同去重键返回已有任务 | `test_dedup_hit_when_backlog_full` |

测试日志（`log.info`，含“判定依据”字样）会打印认领的 offset 列表、
每次 ack 后的 `committed_offset`、在途计数以及最终判定依据，可用
`uv run pytest -o log_cli=true -s` 实时观察。

## 目录结构

```
taskqueue/
  errors.py    # 归一化错误类别
  payload.py   # 负载版本化编解码与校验
  models.py    # Task/Event 数据模型
  store.py     # SQLite 持久化：任务、去重键、位点、事件日志、事件认领表(event_claims)
  queue.py     # 按命名空间的积压限制器（reject/defer）
  engine.py    # 任务引擎：提交、调度、重试、死信、恢复
  consumer.py  # 消费者组：组内并行认领/确认、连续位点、在途上限
  metrics.py   # 吞吐与延迟度量
main.py        # FastAPI HTTP 接口
bench.py       # 吞吐/延迟基准
tests/         # 并行消费、乱序位点、崩溃重投、在途上限、满队列去重、
               # 去重/重试/死信、故障重启、错误分类测试
```

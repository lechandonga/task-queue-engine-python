# 任务队列与事件消费子系统 — 设计文档

本地可运行、进程内（SQLite/WAL + 线程池），目标是在并发、重启、故障场景下
给出一致且可解释的结果。

## 1. 总览

```
提交方 ──► BoundedQueueGate ──► SQLite(tasks) ◄── Dispatcher(worker 线程池)
         （有界准入/去重）          ▲                │ 执行 handler
                                  │                ├─ 成功   → COMPLETED
                                  │                ├─ 可重试 → RETRY_WAIT(退避)
                                  │                └─ 终结   → DEAD（死信表）
事件方 ──► SQLite(events) ◄── ConsumerGroup（组/位点/租约）
```

模块职责：

| 模块 | 职责 |
| --- | --- |
| `taskqueue/storage.py` | SQLite 持久化，所有关键状态变更在单个 IMMEDIATE 事务内 |
| `taskqueue/queue_engine.py` | 有界准入门：长度/积压上限，reject/delay 策略 |
| `taskqueue/dedup.py` | 去重窗口的查询与过期清理（判定与插入在存储事务内） |
| `taskqueue/dispatcher.py` | 认领、执行、超时、退避重试、死信 |
| `taskqueue/consumer.py` | 消费者组、租约认领、连续位点提交 |
| `taskqueue/schema.py` | 任务负载版本化、缺省值与旧版本迁移/拒绝 |
| `taskqueue/errors.py` | 错误归一化（对外稳定的错误类别） |
| `taskqueue/metrics.py` | 吞吐/延迟/拒绝计数（有界内存） |
| `taskqueue/engine.py` | 门面、重启恢复 |
| `taskqueue/api.py` | HTTP 接口与错误→状态码映射 |

## 2. 队列模型

- 任务按 `(namespace, queue)` 组织，**不同 namespace 相互隔离**，queue 是
  namespace 内的进一步分类标签。
- 任务状态机：

```
PENDING ──认领(attempt+1, 加租约)──► RUNNING
  ▲                                   │
  │                                   ├─ 成功（持租约）────────► COMPLETED
  └──── 失败可重试（退避 run_at）─────┤
                                      ├─ 不可重试/超上限（持租约）► DEAD
                                      └─ 租约过期未决 ──► RETRY_WAIT ──► RUNNING
```

- **认领原子性**：`fetch_due_tasks` 用单条 `UPDATE ... WHERE id IN (SELECT ...)`
  在事务内完成“挑选 + 占用 + attempt+1 + 写租约”，多 worker 不会重复认领。
- **租约（visibility timeout）**：认领后任务带 `leased_by` 与
  `lease_expires_ms`。完成/退避/入死信的 SQL 都带 `leased_by=当前worker`
  条件——租约一旦过期被回收重投，原 worker 的迟到结果（成功或失败）不会
  覆盖新状态，从根本上杜绝重复完成与误送死信。

### 2.1 有界资源

| 配置 | 含义 |
| --- | --- |
| `queue.max_pending` | 每个 namespace 的待调度（PENDING/RETRY_WAIT）任务数上限 |
| `queue.max_backlog_bytes` | 每个 namespace 未终态任务的序列化负载字节上限 |
| `queue.overflow_policy` | `reject`（默认，抛 `resource_exhausted`）/ `delay`（阻塞等空位） |

准入判定与任务插入、去重登记在**同一个 IMMEDIATE 事务**内，因此并发提交下
额度统计不会超发。`delay` 模式下提交线程在条件变量上等待，任务离开待调度
集合时被唤醒；`close()` 会唤醒并明确拒绝所有等待者。等待只占用线程栈，
不会无界占用队列内存。

### 2.2 取消

`POST /v1/tasks/{id}/cancel`（或 `engine.cancel_task`）原子地把未决任务
（PENDING/RETRY_WAIT）置为 CANCELLED：

- 已是终态（COMPLETED/DEAD/CANCELLED）→ 返回 `already_terminal`，
  已生效副作用不回滚、不重复；
- 正在 RUNNING → 返回 `running`，不与执行结果竞态（避免重复或卡死），
  调用方可在其结束或租约过期后再决定；
- 取消后任务离开待调度集合，会唤醒 `delay` 策略下等待空位的提交线程。

## 3. 去重规则

- 提交时可选 `dedup_key`。首次提交登记窗口记录
  `expires_at = 提交时刻 + dedup.window_ms`。
- 窗口内（`now < expires_at`）相同 `(namespace, dedup_key)` 的重复提交：
  - **不产生新任务、不执行第二次、不产生重复副作用、不占用队列额度**；
  - 直接返回**首次任务的 id 与状态**（即使首次任务已失败/已完成）。
- 过期边界确定：`expires_at <= now` 即过期；过期记录会在提交时惰性删除，
  或由周期性 `dedup_sweep` 批量清理，之后同 key 可再次提交。
- 时间可注入（引擎 `time_func`），过期语义完全可复现。
- 去重窗口持久化在 SQLite，**重启后窗口内的重复提交仍被拦截**。
- 并发提交同一 key：事务串行判定，只有一个 `inserted`，其余全部 `dedup_hit`。

## 4. 消费者组与位点

- 每个 `(namespace, topic, group)` 维护连续位点 `committed_offset`，
  语义为“**下一条待处理 offset**”，初始为 0；事件 offset 从 0 单调递增。
- 组内多个 consumer 通过**租约**共同认领 `committed_offset` 之后的事件：
  - 同一条事件任一时刻只有一个持租约者；
  - 租约到期未提交的事件可被同组其他消费者重新认领（`attempts` 累加）。
- 位点提交 `commit(offset)` 在单事务内判定并原子推进：

| 情形 | 判定 | 结果 |
| --- | --- | --- |
| `offset == committed_offset` 且提交者持租约 | `committed` | 位点推进到 offset+1 |
| `offset < committed_offset` | `duplicate` | 幂等接受，不改变位点 |
| `offset < 0` | `regression` | 明确拒绝（`precondition_failed`） |
| `offset > committed_offset` | `gap` | 越位/跳位明确拒绝 |
| 未认领或租约不属于提交者 | `not_leased` | 明确拒绝 |

因此**重复提交安全、位点回退/越位被拒绝，绝不会“从中间继续”或跳过事件**。
处理失败时 `process_batch` 停止本批且不提交该事件，事件不会丢失。

- 投递语义为 **at-least-once**：租约超时重投可能让 handler 再次执行，
  “成功处理的副作用只生效一次”需处理器借助**业务幂等键**保证
  （见 `tests/test_concurrency.py` 中的幂等处理器示例）。位点层面保证每个
  offset 恰好被成功提交一次。
- 不同 group 的位点相互独立，各自消费 topic 全量。

## 5. 重试与死信

| 配置 | 含义 |
| --- | --- |
| `retry.max_attempts` | 最大执行次数（含首次），超过后进入死信 |
| `retry.base_delay_ms` / `multiplier` / `max_delay_ms` | 确定性指数退避 |
| `retry.execution_timeout_ms` | 单次执行超时；超时计一次**可重试**失败 |

- 退避公式：`delay = min(base * multiplier**(attempt-1), max)`，**无随机抖动**，
  结果可复现。
- 错误可重试性：
  - `RetryableTaskError`、执行超时、取消 → 可重试；
  - `NonRetryableTaskError` → 直接死信；
  - 其他未知异常 → 归一化为 `internal`，默认**不重试**直接死信
    （避免对未知副作用盲目重复执行）。
- 死信记录（任务 id、queue、负载、尝试次数、归一化原因、时间）写入
  `dead_letters` 表，可按 namespace/queue 分页检索。
- `attempt` 是已发生执行的事实计数，**租约回收与重启都不重置**，
  重试预算跨重启保留，不会因反复崩溃而无限重试。

## 6. 重启恢复

- SQLite 运行于 WAL + `synchronous=FULL`，状态提交即落盘。
- 引擎启动时执行恢复：
  - RUNNING 且租约已过期的任务 → RETRY_WAIT 立即可重投（attempt 保留）；
  - 组内过期的事件认领 → 释放租约可重新认领；
  - 清理已过期的去重键。
- COMPLETED / DEAD 为终态，重启后不变，**已生效副作用不会重复执行**。
- 已完成与未完成的边界：终态由持租约者的原子状态翻转确定；未落终态的
  RUNNING 一律按未完成重投。因此**不丢任务、不重复终态副作用**。

## 7. 负载结构演进

- 每个负载带 `schema_version`（当前为 1）。
- 字段缺失 → 填充稳定缺省值（`args=[]、kwargs={}、metadata={}`）；
  新增字段在 `_FIELD_DEFAULTS` 登记缺省值，旧版本经 `_MIGRATIONS` 升级链
  兼容迁移。
- 记录不是对象、字段类型不符、版本高于当前、或旧版本无迁移链 →
  抛 `invalid_argument` **明确拒绝，绝不静默丢弃**。

## 8. 错误归一化

对外只暴露 `TaskQueueError` 及其类别（`errors.ErrorCategory`）：

| 类别 | 典型场景 | HTTP |
| --- | --- | --- |
| `invalid_argument` | 参数/负载非法 | 400 |
| `not_found` | 任务不存在 | 404 |
| `resource_exhausted` | 队列长度/积压超限 | 429 |
| `conflict` | 冲突 | 409 |
| `precondition_failed` | 位点回退/越位/非租约者提交 | 412 |
| `timeout` | 执行超时 | 504 |
| `cancelled` | 取消 | 499 |
| `retryable` / `non_retryable` | 任务失败归类 | 500 |
| `internal` | 未预期异常（包装，不透出底层类） | 500 |

## 9. 可度量性

`GET /v1/metrics` 暴露：提交数、完成数、重试数、死信数、去重命中数、
拒绝数、事件消费数、位点提交拒绝数，以及完成延迟的 p50/p95/p99/max。
延迟样本保留最近 1024 条（有界 deque），指标仅在事件发生时写入，
**空闲时无后台开销**。

## 10. 配置项一览（`taskqueue/config.py`）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `db_path` | `:memory:` | SQLite 文件路径（持久化请指定文件） |
| `worker_count` | 2 | 后台执行线程数（0 = 仅手动 `run_once` 驱动） |
| `visibility_timeout_ms` | 10000 | 任务/事件认领租约时长，应大于正常处理耗时 |
| `queue.max_pending` | 1000 | 每 namespace 待调度任务上限 |
| `queue.max_backlog_bytes` | 8 MiB | 每 namespace 未终态积压字节上限 |
| `queue.overflow_policy` | `reject` | 超限策略 reject / delay |
| `retry.max_attempts` | 3 | 最大执行次数 |
| `retry.base_delay_ms` | 10 | 首次退避 |
| `retry.multiplier` | 2.0 | 退避乘数 |
| `retry.max_delay_ms` | 1000 | 退避上限 |
| `retry.execution_timeout_ms` | 5000 | 单次执行超时 |
| `dedup.window_ms` | 60000 | 去重窗口有效期 |
| `dedup.sweep_batch_size` | 500 | 清理批量（保留配置） |

## 11. 使用示例

```python
from taskqueue.config import EngineConfig, QueueConfig
from taskqueue.engine import TaskQueueEngine

cfg = EngineConfig(db_path="tasks.db", worker_count=4)
with TaskQueueEngine(cfg, handlers={"email": send_email}) as eng:
    t = eng.submit("shop", {"args": [order_id], "metadata": {"handler": "email"}},
                   queue="mail", dedup_key=f"mail:{order_id}")
    eng.wait_idle()

    # 事件消费
    off = eng.publish_event("shop", "orders", {"order": order_id})
    group = eng.create_consumer_group("shop", "orders", "risk", handle_order)
    group.process_batch(32)
```

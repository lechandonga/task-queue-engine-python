"""队列与消费者相关配置项（均可在创建引擎时覆盖）。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay_ms: int = 10
    max_delay_ms: int = 1000
    multiplier: float = 2.0
    # 单次执行超时；None 表示不限
    execution_timeout_ms: int | None = 5000


@dataclass(frozen=True)
class QueueConfig:
    # 每个命名空间/队列的内存中待调度任务上限
    max_pending: int = 1000
    # 每个队列积压内存字节上限（按序列化负载粗估）
    max_backlog_bytes: int = 8 * 1024 * 1024
    # 超限策略："reject"（拒绝提交）或 "delay"（延迟入队/阻塞等待空位）
    overflow_policy: str = "reject"


@dataclass(frozen=True)
class DedupConfig:
    # 去重窗口有效期；窗口内同 dedup_key 只执行一次
    window_ms: int = 60_000
    # 过期键的清理批量
    sweep_batch_size: int = 500


@dataclass(frozen=True)
class EngineConfig:
    db_path: str = ":memory:"
    worker_count: int = 2
    queue: QueueConfig = QueueConfig()
    retry: RetryConfig = RetryConfig()
    dedup: DedupConfig = DedupConfig()
    # 认领（in-flight）租约：超时未 ack 自动重新投递
    visibility_timeout_ms: int = 10_000

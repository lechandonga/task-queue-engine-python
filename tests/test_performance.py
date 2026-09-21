"""吞吐与延迟基准：结果可度量；空闲与积压时资源占用有界。

不以硬性数字作为通过门槛（CI 机器差异大），而是验证：
- 指标被完整记录（吞吐计数、p50/p95/p99/max 延迟）；
- 批量任务在时限内完成；
- 延迟样本容器有界（不会随任务数无限增长）。
"""

import logging
import resource
import threading

from taskqueue.config import EngineConfig, QueueConfig, RetryConfig
from taskqueue.engine import TaskQueueEngine

log = logging.getLogger("test.perf")


def test_throughput_and_latency_metrics(tmp_path):
    cfg = EngineConfig(
        db_path=str(tmp_path / "p.db"), worker_count=4,
        queue=QueueConfig(max_pending=10_000, max_backlog_bytes=200 * 1024 * 1024),
        retry=RetryConfig(execution_timeout_ms=2000),
    )
    eng = TaskQueueEngine(cfg, handlers={"ok": lambda p: None})
    n = 1000
    with eng:
        for i in range(n):
            eng.submit("ns", {"args": [i], "metadata": {"handler": "ok"}})
        assert eng.wait_idle(timeout=30)
        snap = eng.metrics()
        assert snap["completed"] == n
        for key in ("latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
                    "latency_max_ms"):
            assert key in snap
        log.info("task_id=- offset=- 判定依据=%d 任务完成 completed=%d "
                 "p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms",
                 n, snap["completed"], snap["latency_p50_ms"],
                 snap["latency_p95_ms"], snap["latency_p99_ms"],
                 snap["latency_max_ms"])


def test_memory_stays_bounded_when_idle(tmp_path):
    """空闲时无后台增长：延迟样本有界，RSS 在空闲轮询后不显著膨胀。"""
    cfg = EngineConfig(db_path=str(tmp_path / "p.db"), worker_count=2)
    eng = TaskQueueEngine(cfg, handlers={"ok": lambda p: None})
    with eng:
        for i in range(2000):
            eng.submit("ns", {"args": [i], "metadata": {"handler": "ok"}})
        assert eng.wait_idle(timeout=15)
        # 内部延迟样本上限 1024
        assert len(eng._metrics._m.latencies_ms) <= 1024  # noqa: SLF001
        rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # 再空闲一段时间
        threading.Event().wait(0.2)
        rss_idle = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        log.info("task_id=- offset=- 判定依据=空闲期 RSS 不增长 rss=%d/%d KB",
                 rss_after, rss_idle)
        assert rss_idle <= rss_after + 20_000  # 20MB 容差

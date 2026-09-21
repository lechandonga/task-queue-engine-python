"""本地吞吐/延迟基准：python bench.py [任务数]"""

import sys
import tempfile
import time

from taskqueue.engine import EngineConfig, TaskEngine
from taskqueue.models import TaskStatus


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    db = tempfile.mktemp(suffix=".db")
    eng = TaskEngine(EngineConfig(workers=8, poll_interval_seconds=0.002,
                                  max_queue_size=n + 100), db_path=db)
    eng.register_handler("noop", lambda a: None)
    eng.start()
    try:
        t0 = time.monotonic()
        ids = [eng.submit("bench", {"kind": "noop", "args": {"i": i}})[0]
               for i in range(n)]
        t_submit = time.monotonic() - t0
        deadline = time.monotonic() + 60
        done = 0
        while time.monotonic() < deadline:
            done = sum(1 for i in ids
                       if eng.get_task(i).status is TaskStatus.SUCCEEDED)
            if done == n:
                break
            time.sleep(0.01)
        t_total = time.monotonic() - t0
        snap = eng.metrics.snapshot()
        print(f"tasks={n} done={done}")
        print(f"submit: {n / t_submit:.0f}/s ({t_submit:.3f}s)")
        print(f"e2e:    {n / t_total:.0f}/s ({t_total:.3f}s)")
        print(f"latency_ms: {snap['latency_ms']}")
        # 空闲时资源占用有界：无线程自增、无队列堆积
        import threading
        print(f"threads_idle={threading.active_count()}")
    finally:
        eng.close()


if __name__ == "__main__":
    main()

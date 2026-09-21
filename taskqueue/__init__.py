"""本地可运行的任务队列与事件消费子系统。

对外门面见 :class:`taskqueue.engine.TaskQueueEngine`。
"""

from taskqueue.engine import TaskQueueEngine

__all__ = ["TaskQueueEngine"]

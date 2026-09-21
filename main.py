"""本地启动入口：``uvicorn main:app``。

默认使用进程内引擎（SQLite 文件 ``./taskqueue.db``）；生产可通过
``TASKQUEUE_DB`` 环境变量指定路径。
"""

import os

from taskqueue.api import create_app
from taskqueue.config import EngineConfig
from taskqueue.engine import TaskQueueEngine

_db = os.environ.get("TASKQUEUE_DB", "taskqueue.db")
_engine = TaskQueueEngine(EngineConfig(db_path=_db))
_engine.start()

app = create_app(_engine)

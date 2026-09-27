"""任务队列与事件消费子系统的 HTTP 接口。

错误映射：QueueError.category -> HTTP 状态码，底层异常不透出。
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from taskqueue.consumer import ConsumerGroup
from taskqueue.engine import EngineConfig, TaskEngine
from taskqueue.errors import ErrorCategory, QueueError
from taskqueue.models import Task

_STATUS = {
    ErrorCategory.VALIDATION: 400,
    ErrorCategory.RESOURCE_EXHAUSTED: 429,
    ErrorCategory.CONFLICT: 409,
    ErrorCategory.NOT_FOUND: 404,
    ErrorCategory.INTERNAL: 500,
}


class SubmitRequest(BaseModel):
    namespace: str
    payload: dict
    dedup_key: str | None = None


class EventRequest(BaseModel):
    topic: str
    payload: dict


def create_app(db_path: str | None = None) -> FastAPI:
    engine = TaskEngine(EngineConfig(),
                        db_path or os.environ.get("TQ_DB", ":memory:"))
    consumers: dict[tuple[str, str], ConsumerGroup] = {}
    sinks: dict[tuple[str, str], list] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine.start()
        yield
        engine.close()

    app = FastAPI(title="task-queue-engine", lifespan=lifespan)
    app.state.engine = engine

    @app.exception_handler(QueueError)
    async def queue_error_handler(_: Request, exc: QueueError):
        return JSONResponse(status_code=_STATUS[exc.category],
                            content={"error": exc.to_dict()})

    @app.exception_handler(Exception)
    async def unknown_error_handler(_: Request, exc: Exception):
        # 底层异常归一化为 internal，不透出细节
        return JSONResponse(
            status_code=500,
            content={"error": {"category": "internal",
                               "message": "internal error"}})

    @app.post("/tasks", status_code=201)
    def submit(req: SubmitRequest) -> dict:
        task_id, created = engine.submit(req.namespace, req.payload,
                                         req.dedup_key)
        return {"task_id": task_id, "created": created}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict:
        task = engine.get_task(task_id)
        if task is None:
            from taskqueue.errors import NotFoundError
            raise NotFoundError(f"task {task_id} not found")
        return _task_view(task)

    @app.post("/tasks/{task_id}/cancel")
    def cancel(task_id: str) -> dict:
        engine.cancel(task_id)
        return {"task_id": task_id, "status": "cancelled"}

    @app.get("/dead")
    def list_dead(namespace: str | None = None) -> dict:
        return {"dead": [_task_view(t) for t in engine.list_dead(namespace)]}

    @app.post("/events", status_code=201)
    def append_event(req: EventRequest) -> dict:
        offset = engine.store.append_event(req.topic, req.payload,
                                           time.time())
        return {"topic": req.topic, "offset": offset}

    @app.post("/consumer-groups/{group}/poll")
    def poll(group: str, topic: str, max_events: int = 16,
             max_inflight: int = 32) -> dict:
        key = (group, topic)
        if key not in consumers:
            sink: list = []
            sinks[key] = sink
            consumers[key] = ConsumerGroup(
                engine.store, group, topic, sink.append,
                metrics=engine.metrics, max_inflight=max_inflight)
        processed = consumers[key].poll_once(max_events)
        return {"group": group, "topic": topic, "processed": processed,
                "committed_offset": consumers[key].committed_offset}

    @app.get("/consumer-groups/{group}/sink")
    def sink(group: str, topic: str) -> dict:
        return {"events": sinks.get((group, topic), [])}

    @app.get("/offsets")
    def get_offset(group: str, topic: str) -> dict:
        return {"group": group, "topic": topic,
                "offset": engine.store.get_offset(group, topic)}

    @app.get("/metrics")
    def metrics() -> dict:
        return engine.metrics.snapshot()

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    return app


def _task_view(task: Task) -> dict:
    return {"task_id": task.task_id, "namespace": task.namespace,
            "status": task.status.value, "attempts": task.attempts,
            "payload": task.payload, "dedup_key": task.dedup_key}


app = create_app()

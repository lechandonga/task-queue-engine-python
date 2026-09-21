"""HTTP API（FastAPI）：把错误类别映射为 HTTP 状态码，底层异常不透出。

所有处理函数内部只抛 :class:`TaskQueueError`；未预期异常由兜底处理器转成
500 + INTERNAL，返回体统一为 ``{"error": {"category", "message"}}``。
"""

from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from taskqueue.config import EngineConfig
from taskqueue.errors import (
    ErrorCategory,
    InvalidArgumentError,
    TaskQueueError,
    normalize_error,
)
from taskqueue.engine import TaskQueueEngine

# 错误类别 -> HTTP 状态码
_HTTP_STATUS = {
    ErrorCategory.INVALID_ARGUMENT: 400,
    ErrorCategory.NOT_FOUND: 404,
    ErrorCategory.RESOURCE_EXHAUSTED: 429,
    ErrorCategory.CONFLICT: 409,
    ErrorCategory.PRECONDITION_FAILED: 412,
    ErrorCategory.CANCELLED: 499,
    ErrorCategory.TIMEOUT: 504,
    ErrorCategory.RETRYABLE: 500,
    ErrorCategory.NON_RETRYABLE: 500,
    ErrorCategory.INTERNAL: 500,
}


class SubmitBody(BaseModel):
    namespace: str = Field(min_length=1)
    payload: dict[str, Any]
    queue: str = "default"
    dedup_key: str | None = None
    delay_ms: int = 0


class EventBody(BaseModel):
    payload: dict[str, Any]


class CommitBody(BaseModel):
    group: str
    consumer: str
    offset: int


def create_app(engine: TaskQueueEngine | None = None,
               own_engine: bool = False) -> FastAPI:
    state: dict[str, Any] = {
        "engine": engine,
        "own_engine": own_engine,
        "lock": threading.Lock(),
    }

    def get_engine() -> TaskQueueEngine:
        if state["engine"] is None:
            with state["lock"]:
                if state["engine"] is None:
                    eng = TaskQueueEngine(EngineConfig())
                    eng.start()
                    state["engine"] = eng
                    state["own_engine"] = True
        return state["engine"]

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        if state["own_engine"] and state["engine"] is not None:
            state["engine"].close()
            state["engine"] = None

    app = FastAPI(title="task-queue-engine", lifespan=lifespan)

    # ---- 错误处理 ------------------------------------------------------
    @app.exception_handler(TaskQueueError)
    async def handle_tq_error(_request, exc: TaskQueueError):  # type: ignore[no-untyped-def]
        from fastapi.responses import JSONResponse
        status = _HTTP_STATUS.get(exc.category, 500)
        return JSONResponse(
            status_code=status,
            content={"error": {"category": exc.category.value,
                               "message": exc.message}},
        )

    @app.exception_handler(Exception)
    async def handle_unknown(_request, exc: Exception):  # type: ignore[no-untyped-def]
        from fastapi.responses import JSONResponse
        norm = normalize_error(exc)
        return JSONResponse(
            status_code=500,
            content={"error": {"category": norm.category.value,
                               "message": norm.message}},
        )

    # ---- 任务接口 ------------------------------------------------------
    @app.post("/v1/namespaces/{namespace}/tasks")
    async def submit_task(namespace: str, body: SubmitBody):  # type: ignore[no-untyped-def]
        if body.namespace != namespace:
            raise InvalidArgumentError(
                "namespace in path and body must match if both provided")
        eng = get_engine()
        task = eng.submit(
            namespace, body.payload, queue=body.queue,
            dedup_key=body.dedup_key, delay_ms=body.delay_ms)
        return {"task_id": task.id, "status": task.status.value,
                "dedup_hit": False}

    @app.get("/v1/tasks/{task_id}")
    async def get_task(task_id: str):  # type: ignore[no-untyped-def]
        t = get_engine().get_task(task_id)
        return {
            "task_id": t.id, "namespace": t.namespace, "queue": t.queue,
            "status": t.status.value, "attempt": t.attempt,
            "dedup_key": t.dedup_key, "last_error": t.last_error,
        }

    @app.post("/v1/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str):  # type: ignore[no-untyped-def]
        result = get_engine().cancel_task(task_id)
        return {"task_id": task_id, "result": result}

    @app.get("/v1/namespaces/{namespace}/dead-letters")
    async def list_dead(namespace: str, queue: str | None = None,
                        limit: int = 100, offset: int = 0):  # type: ignore[no-untyped-def]
        rows = get_engine().list_dead_letters(namespace, queue, limit, offset)
        return {"items": [
            {"task_id": d.task_id, "queue": d.queue, "attempts": d.attempts,
             "reason": d.reason, "payload": d.payload,
             "failed_at_ms": d.failed_at_ms}
            for d in rows
        ]}

    # ---- 事件接口 ------------------------------------------------------
    @app.post("/v1/namespaces/{namespace}/topics/{topic}/events")
    async def publish(namespace: str, topic: str, body: EventBody):  # type: ignore[no-untyped-def]
        off = get_engine().publish_event(namespace, topic, body.payload)
        return {"offset": off}

    @app.get("/v1/metrics")
    async def metrics():  # type: ignore[no-untyped-def]
        return get_engine().metrics()

    return app

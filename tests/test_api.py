"""HTTP 接口：错误类别映射、端到端提交-执行-查询、消费位点。"""

import logging
import time

import pytest
from fastapi.testclient import TestClient

from main import create_app

log = logging.getLogger("test.api")


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("TQ_DB", str(tmp_path / "api.db"))
    app = create_app(str(tmp_path / "api.db"))
    with TestClient(app) as c:
        yield c


def test_submit_and_execute(client):
    engine = client.app.state.engine
    seen = []
    engine.register_handler("add", lambda a: seen.append(a["n"]))
    r = client.post("/tasks", json={"namespace": "ns",
                                    "payload": {"kind": "add",
                                                "args": {"n": 7}}})
    assert r.status_code == 201
    task_id = r.json()["task_id"]
    log.info("submitted task=%s", task_id)
    deadline = time.time() + 5
    while time.time() < deadline:
        body = client.get(f"/tasks/{task_id}").json()
        if body["status"] == "succeeded":
            break
        time.sleep(0.02)
    assert body["status"] == "succeeded" and seen == [7]
    log.info("task=%s status=%s (判定依据: 终态可查询)", task_id,
             body["status"])


def test_dedup_via_api(client):
    payload = {"kind": "noop", "args": {}}
    client.app.state.engine.register_handler("noop", lambda a: None)
    r1 = client.post("/tasks", json={"namespace": "ns", "payload": payload,
                                     "dedup_key": "k"})
    r2 = client.post("/tasks", json={"namespace": "ns", "payload": payload,
                                     "dedup_key": "k"})
    assert r1.json()["created"] is True
    assert r2.json()["created"] is False
    assert r1.json()["task_id"] == r2.json()["task_id"]
    log.info("dedup via api: same task_id=%s", r1.json()["task_id"])


def test_validation_error_mapped(client):
    r = client.post("/tasks", json={"namespace": "ns",
                                    "payload": {"args": {}}})
    assert r.status_code == 400
    body = r.json()["error"]
    log.info("validation mapped: %s", body)
    assert body["category"] == "validation"
    assert "Traceback" not in str(body)  # 底层细节不透出


def test_not_found_mapped(client):
    r = client.get("/tasks/does-not-exist")
    assert r.status_code == 404
    assert r.json()["error"]["category"] == "not_found"


def test_resource_exhausted_mapped(client):
    engine = client.app.state.engine
    engine.set_namespace_limit("tiny", max_tasks=1, max_bytes=1 << 20)
    import threading
    gate = threading.Event()
    engine.register_handler("slow", lambda a: gate.wait(3))
    try:
        client.post("/tasks", json={"namespace": "tiny",
                                    "payload": {"kind": "slow", "args": {}}})
        r = client.post("/tasks", json={"namespace": "tiny",
                                        "payload": {"kind": "slow",
                                                    "args": {}}})
        assert r.status_code == 429
        assert r.json()["error"]["category"] == "resource_exhausted"
        log.info("backpressure mapped: %s", r.json()["error"])
    finally:
        gate.set()


def test_events_offsets_and_consume(client):
    for i in range(5):
        r = client.post("/events", json={"topic": "tp",
                                         "payload": {"n": i}})
        assert r.status_code == 201 and r.json()["offset"] == i
    r = client.post("/consumer-groups/g1/poll", params={"topic": "tp",
                                                        "max_events": 3})
    assert r.json()["processed"] == 3 and r.json()["committed_offset"] == 3
    r = client.get("/offsets", params={"group": "g1", "topic": "tp"})
    assert r.json()["offset"] == 3
    sink = client.get("/consumer-groups/g1/sink",
                      params={"topic": "tp"}).json()["events"]
    log.info("consumed sink=%s offset=3 (判定依据: 位点与已消费一致)", sink)
    assert [e["n"] for e in sink] == [0, 1, 2]
    r = client.post("/consumer-groups/g1/poll", params={"topic": "tp"})
    assert r.json()["processed"] == 2 and r.json()["committed_offset"] == 5


def test_metrics_endpoint(client):
    engine = client.app.state.engine
    engine.register_handler("noop", lambda a: None)
    client.post("/tasks", json={"namespace": "ns",
                                "payload": {"kind": "noop", "args": {}}})
    deadline = time.time() + 5
    while time.time() < deadline:
        m = client.get("/metrics").json()
        if m["counters"].get("succeeded", 0) >= 1:
            break
        time.sleep(0.02)
    log.info("metrics: %s", m)
    assert m["counters"]["submitted"] >= 1
    assert "latency_ms" in m and "throughput_per_second" in m

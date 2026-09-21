"""负载结构演进测试：缺省语义稳定、旧版本兼容、非法记录明确拒绝。"""

import logging

import pytest

from taskqueue.errors import InvalidArgumentError
from taskqueue.schema import CURRENT_SCHEMA_VERSION, TaskPayload

log = logging.getLogger("test.schema")


def test_missing_fields_get_stable_defaults():
    p = TaskPayload.from_dict({})
    log.info("task_id=- offset=- 判定依据=字段缺失补稳定缺省 args=%s kwargs=%s", p.args, p.kwargs)
    assert p.args == ()
    assert p.kwargs == {}
    assert p.metadata == {}
    assert p.schema_version == CURRENT_SCHEMA_VERSION


def test_roundtrip():
    p = TaskPayload(args=(1, "x"), kwargs={"a": 2}, metadata={"handler": "echo"})
    q = TaskPayload.from_dict(p.to_dict())
    assert p == q


@pytest.mark.parametrize(
    "bad",
    [
        {"args": "not-a-list"},
        {"kwargs": [1, 2]},
        {"metadata": "nope"},
        {"schema_version": "1"},
        {"schema_version": True},
        {"schema_version": 999},
    ],
)
def test_invalid_records_are_rejected_not_dropped(bad):
    with pytest.raises(InvalidArgumentError) as ei:
        TaskPayload.from_dict(bad)
    log.info("task_id=- offset=- 判定依据=非法负载拒绝 raw=%s -> %s", bad, ei.value.message)


def test_non_object_payload_rejected():
    with pytest.raises(InvalidArgumentError):
        TaskPayload.from_dict([1, 2, 3])


def test_unknown_old_version_without_migration_rejected():
    # 当前只有 v1，没有 v0 的迁移链，必须明确报错
    with pytest.raises(InvalidArgumentError):
        TaskPayload.from_dict({"schema_version": 0})

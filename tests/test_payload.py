"""负载结构演进：缺省语义、版本兼容、字段缺失与类型不符。"""

import json
import logging

import pytest

from taskqueue import payload
from taskqueue.errors import ValidationError

log = logging.getLogger("test.payload")


def test_roundtrip_current_version():
    blob = payload.encode({"kind": "email", "args": {"to": "a@b"},
                           "priority": 3})
    out = payload.decode(blob)
    log.info("roundtrip payload=%s", out)
    assert out == {"kind": "email", "args": {"to": "a@b"}, "priority": 3}


def test_old_version_gets_stable_defaults():
    old = json.dumps({"v": 1, "data": {"kind": "email"}}).encode()
    out = payload.decode(old)
    log.info("v1 payload migrated: %s (判定依据: 新增字段补稳定缺省)", out)
    assert out == {"kind": "email", "args": {}, "priority": 0}


def test_newer_version_rejected():
    blob = json.dumps({"v": 99, "data": {"kind": "x"}}).encode()
    with pytest.raises(ValidationError) as ei:
        payload.decode(blob)
    log.info("newer version rejected: %s", ei.value.message)


def test_missing_required_field_rejected():
    with pytest.raises(ValidationError) as ei:
        payload.encode({"args": {}})
    log.info("missing field rejected: %s", ei.value.message)


def test_type_mismatch_rejected():
    with pytest.raises(ValidationError) as ei:
        payload.encode({"kind": "x", "priority": "high"})
    log.info("type mismatch rejected: %s", ei.value.message)


def test_bool_not_accepted_as_int():
    with pytest.raises(ValidationError):
        payload.encode({"kind": "x", "priority": True})


def test_unknown_fields_rejected_not_dropped():
    with pytest.raises(ValidationError) as ei:
        payload.encode({"kind": "x", "mystery": 1})
    log.info("unknown field rejected (不静默丢弃): %s", ei.value.message)


def test_garbage_bytes_rejected():
    with pytest.raises(ValidationError):
        payload.decode(b"\x00\x01not json")

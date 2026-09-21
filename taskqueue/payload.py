"""任务负载的版本化编解码。

规则：
- 编码时写入 schema 版本号；解码时按版本迁移到当前版本。
- 比当前更新的版本：明确拒绝（ValidationError），不静默尝试。
- 字段缺失：有缺省值则补稳定缺省；无缺省值则报错，不静默丢弃。
- 类型不符：报错，不静默丢弃。
"""

from __future__ import annotations

import json
from typing import Any, Callable

from .errors import ValidationError

SCHEMA_VERSION = 2  # 当前负载结构版本

_REQUIRED = object()


class Field:
    """单个字段的约束：类型与缺省语义。"""

    def __init__(self, type_: type | tuple[type, ...], default: Any = _REQUIRED,
                 since: int = 1) -> None:
        self.type = type_
        self.default = default
        self.since = since  # 该字段自哪个版本引入


# 当前版本（v2）的负载结构。v1 没有 "priority" 字段，迁移时补稳定缺省 0。
SCHEMA: dict[str, Field] = {
    "kind": Field(str),
    "args": Field(dict, default=dict),
    "priority": Field(int, default=0, since=2),
}

# 版本迁移函数：migrate[v] 把 v 版本的数据升级为 v+1。
_MIGRATIONS: dict[int, Callable[[dict], dict]] = {
    1: lambda d: d,  # v1 -> v2：仅补缺省，由 validate 统一处理
}


def encode(payload: dict) -> bytes:
    """校验并编码为字节。结构不合法时抛 ValidationError。"""
    normalized = validate(payload)
    return json.dumps({"v": SCHEMA_VERSION, "data": normalized},
                      sort_keys=True).encode("utf-8")


def decode(data: bytes) -> dict:
    """解码并迁移到当前版本。版本过新或结构不合法时明确拒绝。"""
    try:
        raw = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"payload is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict) or "v" not in raw or "data" not in raw:
        raise ValidationError("payload envelope must contain 'v' and 'data'")
    version = raw["v"]
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValidationError("payload version must be an integer")
    if version < 1:
        raise ValidationError(f"unsupported payload version: {version}")
    if version > SCHEMA_VERSION:
        raise ValidationError(
            f"payload version {version} is newer than supported {SCHEMA_VERSION}")
    body = raw["data"]
    if not isinstance(body, dict):
        raise ValidationError("payload data must be an object")
    while version < SCHEMA_VERSION:
        body = _MIGRATIONS[version](body)
        version += 1
    return validate(body)


def validate(payload: dict) -> dict:
    """按当前结构校验并归一化：补缺省、查类型，不静默丢字段。"""
    if not isinstance(payload, dict):
        raise ValidationError("payload must be an object")
    out: dict[str, Any] = {}
    for name, spec in SCHEMA.items():
        if name in payload:
            value = payload[name]
            if not _type_ok(value, spec.type):
                raise ValidationError(
                    f"field '{name}' must be {_type_name(spec.type)}, "
                    f"got {type(value).__name__}")
            out[name] = value
        elif spec.default is not _REQUIRED:
            default = spec.default
            out[name] = default() if callable(default) else default
        else:
            raise ValidationError(f"required field '{name}' is missing")
    unknown = set(payload) - set(SCHEMA)
    if unknown:
        raise ValidationError(f"unknown fields: {sorted(unknown)}")
    return out


def _type_ok(value: Any, type_: type | tuple[type, ...]) -> bool:
    # bool 是 int 的子类，需排除，避免 True 被当作合法 int。
    if type_ is int and isinstance(value, bool):
        return False
    return isinstance(value, type_)


def _type_name(type_: type | tuple[type, ...]) -> str:
    if isinstance(type_, tuple):
        return "|".join(t.__name__ for t in type_)
    return type_.__name__

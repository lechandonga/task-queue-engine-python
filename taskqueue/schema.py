"""任务负载的结构演进（schema versioning）。

- 每个 :class:`TaskPayload` 带 ``schema_version``。
- 新增字段必须给出稳定的缺省值；旧版本负载被兼容升级，或明确拒绝。
- 字段缺失/类型不符抛出 :class:`InvalidArgumentError`，绝不静默丢弃。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from taskqueue.errors import InvalidArgumentError

CURRENT_SCHEMA_VERSION = 1

# 字段缺省值演进表：新增字段在此登记，保证“新增字段缺省语义稳定”。
# 形如 {引入版本号: {字段: 缺省值}}
_FIELD_DEFAULTS: dict[int, dict[str, Any]] = {}

# 各版本向前兼容升级链：{from_version: to_version}
_MIGRATIONS: dict[int, int] = {}


def _migrate(raw: dict[str, Any]) -> dict[str, Any]:
    """把旧版本记录逐级升级到当前版本。

    未知的旧版本且没有迁移链 -> 明确拒绝；绝不静默丢弃。
    """
    version = raw.get("schema_version", 1)
    if not isinstance(version, int) or isinstance(version, bool):
        raise InvalidArgumentError(f"schema_version must be int, got {type(version).__name__}")
    if version > CURRENT_SCHEMA_VERSION:
        raise InvalidArgumentError(
            f"schema_version {version} is newer than supported {CURRENT_SCHEMA_VERSION}"
        )
    seen: set[int] = set()
    while version < CURRENT_SCHEMA_VERSION:
        if version in seen or version not in _MIGRATIONS:
            raise InvalidArgumentError(f"no migration path from schema_version {version}")
        seen.add(version)
        target = _MIGRATIONS[version]
        defaults = _FIELD_DEFAULTS.get(target, {})
        for name, default in defaults.items():
            raw.setdefault(name, default if not callable(default) else default())
        version = target
        raw["schema_version"] = version
    raw["schema_version"] = CURRENT_SCHEMA_VERSION
    return raw


@dataclass(frozen=True)
class TaskPayload:
    """任务负载（不可变值对象）。

    ``args`` / ``kwargs`` 为处理器入参；``metadata`` 为可演进的扩展字段。
    """

    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: int = CURRENT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "metadata": dict(self.metadata),
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, raw: Any) -> TaskPayload:
        """从外部记录解析负载：校验类型、迁移旧版本，拒绝非法记录。

        - 必须是 dict；
        - ``args`` 必须是 list（缺省为空），``kwargs``/``metadata`` 必须是 dict；
        - 字段缺失 -> 填稳定缺省值；类型不符 -> :class:`InvalidArgumentError`。
        """
        if not isinstance(raw, dict):
            raise InvalidArgumentError(f"payload must be an object, got {type(raw).__name__}")
        data = _migrate(dict(raw))

        args_raw = data.get("args", [])
        if not isinstance(args_raw, list):
            raise InvalidArgumentError("payload.args must be a list")
        kwargs_raw = data.get("kwargs", {})
        if not isinstance(kwargs_raw, dict):
            raise InvalidArgumentError("payload.kwargs must be an object")
        metadata_raw = data.get("metadata", {})
        if not isinstance(metadata_raw, dict):
            raise InvalidArgumentError("payload.metadata must be an object")

        return cls(
            args=tuple(args_raw),
            kwargs=dict(kwargs_raw),
            metadata=dict(metadata_raw),
            schema_version=CURRENT_SCHEMA_VERSION,
        )

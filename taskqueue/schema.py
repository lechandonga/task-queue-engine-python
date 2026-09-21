"""任务负载的结构演进（schema versioning）。

- 每个 :class:`TaskPayload` 带 ``schema_version``。
- 新增字段必须给出稳定的缺省值；旧版本负载被兼容升级，或明确拒绝。
- 字段缺失/类型不符抛出 :class:`InvalidArgumentError`，绝不静默丢弃。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

CURRENT_SCHEMA_VERSION = 1

# 各版本向前兼容升级链：{1: 2, ...}
_MIGRATIONS: dict[int, Any] = {}


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
        raise NotImplementedError

    @classmethod
    def from_dict(cls, raw: Any) -> TaskPayload:
        """从外部记录解析负载：校验类型、迁移旧版本，拒绝非法记录。"""
        raise NotImplementedError

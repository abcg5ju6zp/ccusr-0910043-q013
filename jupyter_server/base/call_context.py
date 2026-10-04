"""项目内部接口说明。"""
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from contextvars import Context, ContextVar, copy_context
from typing import Any


class CallContext:
    """项目内部接口说明。"""

    # Add well-known (file-spanning) names here.
    #: Provides access to the current request handler once set.
    JUPYTER_HANDLER: str = "JUPYTER_HANDLER"

    # A map of variable name to value is maintained as the single ContextVar.  This also enables
    # easier management over maintaining a set of ContextVar instances, since the Context is a
    # map of ContextVar instances to their values, and the "name" is no longer a lookup key.
    _NAME_VALUE_MAP = "_name_value_map"
    _name_value_map: ContextVar[dict[str, Any]] = ContextVar(_NAME_VALUE_MAP)

    @classmethod
    def get(cls, name: str) -> Any:
        """项目内部接口说明。"""
        name_value_map = CallContext._get_map()
        if name in name_value_map:
            return name_value_map[name]
        return None  # TODO: should this raise `LookupError` (or a custom error derived from said)

    @classmethod
    def set(cls, name: str, value: Any) -> None:
        """项目内部接口说明。"""
        name_value_map = CallContext._get_map().copy()
        name_value_map[name] = value
        CallContext._name_value_map.set(name_value_map)

    @classmethod
    def context_variable_names(cls) -> list[str]:
        """项目内部接口说明。"""
        name_value_map = CallContext._get_map()
        return list(name_value_map.keys())

    @classmethod
    def _get_map(cls) -> dict[str, Any]:
        """项目内部接口说明。"""
        ctx: Context = copy_context()
        if CallContext._name_value_map not in ctx:
            CallContext._name_value_map.set({})
        return CallContext._name_value_map.get()

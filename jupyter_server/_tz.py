"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo

# constant for zero offset
ZERO = timedelta(0)


class tzUTC(tzinfo):  # noqa: N801
    """项目内部接口说明。"""

    def utcoffset(self, d: datetime | None) -> timedelta:
        """项目内部接口说明。"""
        return ZERO

    def dst(self, d: datetime | None) -> timedelta:
        """项目内部接口说明。"""
        return ZERO


def utcnow() -> datetime:
    """项目内部接口说明。"""
    return datetime.now(timezone.utc)


def utcfromtimestamp(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, timezone.utc)


UTC = tzUTC()  # type:ignore[abstract]


def isoformat(dt: datetime) -> str:
    """项目内部接口说明。"""
    return dt.isoformat().replace("+00:00", "Z")

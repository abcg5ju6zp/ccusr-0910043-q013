"""项目内部接口说明。"""

from .metrics import HTTP_REQUEST_DURATION_SECONDS  # type:ignore[unused-ignore]


def prometheus_log_method(handler):
    """项目内部接口说明。"""
    HTTP_REQUEST_DURATION_SECONDS.labels(
        method=handler.request.method,
        handler=f"{handler.__class__.__module__}.{type(handler).__name__}",
        status_code=handler.get_status(),
    ).observe(handler.request.request_time())

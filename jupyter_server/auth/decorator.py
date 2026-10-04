"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import asyncio
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, cast

from jupyter_core.utils import ensure_async
from tornado.log import app_log
from tornado.web import HTTPError

from .utils import HTTP_METHOD_TO_AUTH_ACTION

FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def authorized(
    action: str | FuncT | None = None,
    resource: str | None = None,
    message: str | None = None,
) -> FuncT:
    """项目内部接口说明。"""

    def wrapper(method):
        @wraps(method)
        async def inner(self, *args, **kwargs):
            # default values for action, resource
            nonlocal action
            nonlocal resource
            nonlocal message
            if action is None:
                http_method = self.request.method.upper()
                action = HTTP_METHOD_TO_AUTH_ACTION[http_method]
            if resource is None:
                resource = self.auth_resource
            if message is None:
                message = f"User is not authorized to {action} on resource: {resource}."

            user = self.current_user
            if not user:
                app_log.warning("Attempting to authorize request without authentication!")
                raise HTTPError(status_code=403, log_message=message)
            # If the user is allowed to do this action,
            # call the method.
            authorized = await ensure_async(
                self.authorizer.is_authorized(self, user, action, resource)
            )
            if authorized:
                out = method(self, *args, **kwargs)
                # If the method is a coroutine, await it
                if asyncio.iscoroutine(out):
                    return await out
                return out
            # else raise an exception.
            else:
                raise HTTPError(status_code=403, log_message=message)

        return inner

    if callable(action):
        method = action
        action = None
        # no-arguments `@authorized` decorator called
        return cast("FuncT", wrapper(method))

    return cast("FuncT", wrapper)


def allow_unauthenticated(method: FuncT) -> FuncT:
    """项目内部接口说明。"""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        return method(self, *args, **kwargs)

    setattr(wrapper, "__allow_unauthenticated", True)

    return cast("FuncT", wrapper)


def ws_authenticated(method: FuncT) -> FuncT:
    """项目内部接口说明。"""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        user = self.current_user
        if user is None:
            self.log.warning("Couldn't authenticate WebSocket connection")
            raise HTTPError(403)
        return method(self, *args, **kwargs)

    setattr(wrapper, "__allow_unauthenticated", False)

    return cast("FuncT", wrapper)

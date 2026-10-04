"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

from typing import TYPE_CHECKING

from traitlets import Instance
from traitlets.config import LoggingConfigurable

from .identity import IdentityProvider, User

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from jupyter_server.base.handlers import JupyterHandler


class Authorizer(LoggingConfigurable):
    """项目内部接口说明。"""

    identity_provider = Instance(IdentityProvider)

    def is_authorized(
        self, handler: JupyterHandler, user: User, action: str, resource: str
    ) -> Awaitable[bool] | bool:
        """项目内部接口说明。"""
        raise NotImplementedError


class AllowAllAuthorizer(Authorizer):
    """项目内部接口说明。"""

    def is_authorized(
        self, handler: JupyterHandler, user: User, action: str, resource: str
    ) -> bool:
        """项目内部接口说明。"""
        return True

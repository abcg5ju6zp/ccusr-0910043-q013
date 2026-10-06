"""扩展配置变更单的运维 HTTP 接口。

端点：

- ``GET  /api/extension-changes``：列出全部变更单、当前已提交快照版本、
  正在执行的变更单号与各版本快照的在读读者数；
- ``POST /api/extension-changes``：提交变更单（幂等），或携带
  ``"dry_run": true`` 仅执行冻结与预检；
- ``GET  /api/extension-changes/<change_id>``：查询一张变更单的完整记录，
  包括每个阶段、每个扩展的真实执行结果；
- ``POST /api/extension-changes/<change_id>/rollback``：回退一张已提交的
  变更单（以反向变更单的形式走完整事务流水线）。
"""

from __future__ import annotations

import json

from tornado import web

from jupyter_server.auth.decorator import authorized

from ...base.handlers import APIHandler
from .coordinator import (
    ChangeConflictError,
    ChangeInProgressError,
    ChangeNotFoundError,
)
from .models import ChangeState

AUTH_RESOURCE = "config"


class ExtensionConfigTxnBaseHandler(APIHandler):
    """变更单接口的公共基类。"""

    auth_resource = AUTH_RESOURCE

    @property
    def txn_manager(self):
        """当前 ServerApp 的扩展配置事务管理器。"""
        manager = self.settings.get("extension_config_txn_manager")
        if manager is None:
            raise web.HTTPError(503, "扩展配置事务管理器不可用")
        return manager

    def _finish_order(self, order, created: bool) -> None:
        """按变更单终态映射 HTTP 状态码并返回记录。"""
        if order.state == ChangeState.COMMITTED:
            self.set_status(201 if created else 200)
        elif order.state == ChangeState.REJECTED:
            self.set_status(422)
        else:
            # ROLLED_BACK / RECOVERY_REQUIRED：系统处于一致状态或现场已保留，
            # 记录中带有每个阶段的真实结果
            self.set_status(500)
        self.finish(json.dumps(order.to_dict()))

    def _handle_error(self, e: Exception) -> None:
        """把协调器异常映射为 HTTP 错误。"""
        if isinstance(e, ChangeNotFoundError):
            raise web.HTTPError(404, str(e)) from e
        if isinstance(e, (ChangeConflictError, ChangeInProgressError)):
            raise web.HTTPError(409, str(e)) from e
        if isinstance(e, ValueError):
            raise web.HTTPError(400, str(e)) from e
        raise e


class ExtensionChangesHandler(ExtensionConfigTxnBaseHandler):
    """变更单集合接口。"""

    @web.authenticated
    @authorized
    def get(self):
        """列出全部变更单与当前快照状态。"""
        manager = self.txn_manager
        payload = {
            "committed_version": manager.current_snapshot().version,
            "active_change_id": manager.active_change_id,
            "active_readers": manager.active_readers,
            "orders": [order.to_dict() for order in manager.list_orders()],
        }
        self.finish(json.dumps(payload))

    @web.authenticated
    @authorized
    async def post(self):
        """提交一张变更单（幂等）；``dry_run`` 时仅做冻结与预检。"""
        manager = self.txn_manager
        body = self.get_json_body()
        if not isinstance(body, dict):
            raise web.HTTPError(400, "请求体必须是 JSON 对象")
        changes = body.get("changes")
        if body.get("dry_run"):
            try:
                report = manager.preflight(changes)
            except Exception as e:
                self._handle_error(e)
                return
            self.finish(json.dumps(report))
            return
        change_id = body.get("change_id")
        if not isinstance(change_id, str) or not change_id:
            raise web.HTTPError(400, "请求体必须包含非空的 change_id")
        created = not manager.has_order(change_id)
        try:
            order = await manager.submit(change_id, changes)
        except Exception as e:
            self._handle_error(e)
            return
        self._finish_order(order, created)


class ExtensionChangeHandler(ExtensionConfigTxnBaseHandler):
    """单张变更单接口。"""

    @web.authenticated
    @authorized
    def get(self, change_id):
        """查询一张变更单的完整记录。"""
        try:
            order = self.txn_manager.get_order(change_id)
        except Exception as e:
            self._handle_error(e)
            return
        self.finish(json.dumps(order.to_dict()))


class ExtensionChangeRollbackHandler(ExtensionConfigTxnBaseHandler):
    """已提交变更单的回退接口。"""

    @web.authenticated
    @authorized
    async def post(self, change_id):
        """回退一张已提交的变更单。"""
        manager = self.txn_manager
        inverse_id = f"{change_id}:rollback"
        created = not manager.has_order(inverse_id)
        try:
            order = await manager.rollback_committed(change_id)
        except Exception as e:
            self._handle_error(e)
            return
        self._finish_order(order, created)


# URL 到处理器的映射

change_id_regex = r"(?P<change_id>[\w.\-:]+)"

default_handlers = [
    (r"/api/extension-changes", ExtensionChangesHandler),
    (rf"/api/extension-changes/{change_id_regex}/rollback", ExtensionChangeRollbackHandler),
    (rf"/api/extension-changes/{change_id_regex}", ExtensionChangeHandler),
]

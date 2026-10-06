"""扩展配置事务化变更的 REST 接口。

运维通过本接口提交变更单、分阶段推进并查询每个阶段的真实结果:

* ``GET  /api/extension-config/changes`` —— 全量变更单登记簿;
* ``POST /api/extension-config/changes`` —— 提交变更单
  (``{"change_id", "changes": {扩展名: 配置}, "auto_commit"?}``);
* ``GET  /api/extension-config/changes/<change_id>`` —— 单张变更单状态;
* ``POST /api/extension-config/changes/<change_id>/commit`` —— 共同提交;
* ``POST /api/extension-config/changes/<change_id>/rollback`` —— 回退。
"""

from __future__ import annotations

import json

from tornado import web

from jupyter_server.auth.decorator import authorized
from jupyter_server.base.handlers import APIHandler

from .rollout import (
    ChangeOrderConflict,
    ChangeStage,
    InvalidStageTransition,
    UnknownChangeOrder,
)

AUTH_RESOURCE = "config"


class RolloutBaseHandler(APIHandler):
    """变更单接口的公共基类。"""

    auth_resource = AUTH_RESOURCE

    @property
    def coordinator(self):
        """当前 ServerApp 装配的变更协调器。"""
        return self.settings["extension_config_coordinator"]

    def _finish_json(self, payload, status=200):
        """以 JSON 形式返回结果。"""
        self.set_header("Content-Type", "application/json")
        self.set_status(status)
        self.finish(json.dumps(payload))


class ChangeOrdersHandler(RolloutBaseHandler):
    """变更单的查询与提交。"""

    @web.authenticated
    @authorized
    def get(self):
        """返回全量变更单登记簿及当前生效快照版本。"""
        self._finish_json(self.coordinator.status())

    @web.authenticated
    @authorized
    def post(self):
        """提交一张变更单。

        幂等:同一 change_id 且负载一致时返回既有结果(200);
        负载不一致返回 409;预检失败返回 422 及预检问题清单。
        """
        body = self.get_json_body()
        if not isinstance(body, dict):
            raise web.HTTPError(400, reason="请求体必须是 JSON 对象")
        change_id = body.get("change_id")
        changes = body.get("changes")
        auto_commit = bool(body.get("auto_commit", True))
        if not change_id or not isinstance(change_id, str):
            raise web.HTTPError(400, reason="缺少有效的 change_id")
        if not isinstance(changes, dict) or not changes:
            raise web.HTTPError(400, reason="changes 必须是非空的 {扩展名: 配置} 映射")
        for extension, config in changes.items():
            if not isinstance(config, dict):
                raise web.HTTPError(400, reason=f"扩展 {extension} 的配置必须是 JSON 对象")
        try:
            record = self.coordinator.submit(change_id, changes, auto_commit=auto_commit)
        except ChangeOrderConflict as e:
            raise web.HTTPError(409, reason=str(e)) from e
        status = 201
        if record.stage is ChangeStage.REJECTED:
            status = 422
        self._finish_json(record.to_dict(), status=status)


class ChangeOrderHandler(RolloutBaseHandler):
    """单张变更单的状态查询。"""

    @web.authenticated
    @authorized
    def get(self, change_id):
        """返回一张变更单每个阶段、每个参与者的真实结果。"""
        try:
            record = self.coordinator.status(change_id)
        except UnknownChangeOrder as e:
            raise web.HTTPError(404, reason=str(e)) from e
        self._finish_json(record)


class ChangeOrderActionHandler(RolloutBaseHandler):
    """对已就绪的变更单执行分阶段动作(commit / rollback)。"""

    @web.authenticated
    @authorized
    def post(self, change_id, action):
        """推进或回退一张变更单。"""
        try:
            if action == "commit":
                record = self.coordinator.commit(change_id)
            elif action == "rollback":
                record = self.coordinator.rollback(change_id)
            else:
                raise web.HTTPError(404, reason=f"未知操作: {action}")
        except UnknownChangeOrder as e:
            raise web.HTTPError(404, reason=str(e)) from e
        except InvalidStageTransition as e:
            raise web.HTTPError(409, reason=str(e)) from e
        self._finish_json(record.to_dict())


# URL 到处理器的映射

change_id_regex = r"(?P<change_id>[^/]+)"
action_regex = r"(?P<action>commit|rollback)"

default_handlers = [
    (r"/api/extension-config/changes", ChangeOrdersHandler),
    (r"/api/extension-config/changes/%s" % change_id_regex, ChangeOrderHandler),
    (
        r"/api/extension-config/changes/%s/%s" % (change_id_regex, action_regex),
        ChangeOrderActionHandler,
    ),
]

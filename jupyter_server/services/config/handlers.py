"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import json

from tornado import web

from jupyter_server.auth.decorator import authorized

from ...base.handlers import APIHandler

AUTH_RESOURCE = "config"


class ConfigHandler(APIHandler):
    """项目内部接口说明。"""

    auth_resource = AUTH_RESOURCE

    def _check_no_active_config_transaction(self):
        """变更单事务执行期间，拒绝绕过事务的直写，保证可恢复状态不被破坏。"""
        manager = self.settings.get("extension_config_txn_manager")
        if manager is not None and manager.transaction_active:
            raise web.HTTPError(
                409,
                "extension config change %r in progress; "
                "direct writes are rejected until it finishes" % manager.active_change_id,
            )

    @web.authenticated
    @authorized
    def get(self, section_name):
        """项目内部接口说明。"""
        self.set_header("Content-Type", "application/json")
        self.finish(json.dumps(self.config_manager.get(section_name)))

    @web.authenticated
    @authorized
    def put(self, section_name):
        """项目内部接口说明。"""
        self._check_no_active_config_transaction()
        data = self.get_json_body()  # Will raise 400 if content is not valid JSON
        self.config_manager.set(section_name, data)
        self.set_status(204)

    @web.authenticated
    @authorized
    def patch(self, section_name):
        """项目内部接口说明。"""
        self._check_no_active_config_transaction()
        new_data = self.get_json_body()
        section = self.config_manager.update(section_name, new_data)
        self.finish(json.dumps(section))


# URL to handler mappings

section_name_regex = r"(?P<section_name>\w+)"

default_handlers = [
    (r"/api/config/%s" % section_name_regex, ConfigHandler),
]

"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from tornado import web

from jupyter_server.auth.decorator import authorized

from ...base.handlers import APIHandler
from . import csp_report_uri

AUTH_RESOURCE = "csp"


class CSPReportHandler(APIHandler):
    """项目内部接口说明。"""

    auth_resource = AUTH_RESOURCE
    _track_activity = False

    def skip_check_origin(self):
        """项目内部接口说明。"""
        return True

    def check_xsrf_cookie(self):
        """项目内部接口说明。"""
        return

    @web.authenticated
    @authorized
    def post(self):
        """项目内部接口说明。"""
        self.log.warning(
            "Content security violation: %s",
            self.request.body.decode("utf8", "replace"),
        )


default_handlers = [(csp_report_uri, CSPReportHandler)]

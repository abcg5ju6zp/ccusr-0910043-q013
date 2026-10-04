"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from ..base.handlers import JupyterHandler
from .decorator import allow_unauthenticated


class LogoutHandler(JupyterHandler):
    """项目内部接口说明。"""

    @allow_unauthenticated
    def get(self):
        """项目内部接口说明。"""
        self.identity_provider.clear_login_cookie(self)
        if self.login_available:
            message = {"info": "Successfully logged out."}
        else:
            message = {"warning": "Cannot log out. Jupyter Server authentication is disabled."}
        self.write(self.render_template("logout.html", message=message))


default_handlers = [(r"/logout", LogoutHandler)]

"""项目内部接口说明。"""

from tornado import ioloop, web

from jupyter_server.auth.decorator import authorized
from jupyter_server.base.handlers import JupyterHandler

AUTH_RESOURCE = "server"


class ShutdownHandler(JupyterHandler):
    """项目内部接口说明。"""

    auth_resource = AUTH_RESOURCE

    @web.authenticated
    @authorized
    async def post(self):
        """项目内部接口说明。"""
        self.log.info("Shutting down on /api/shutdown request.")

        if self.serverapp:
            await self.serverapp._cleanup()

        ioloop.IOLoop.current().stop()


default_handlers = [
    (r"/api/shutdown", ShutdownHandler),
]

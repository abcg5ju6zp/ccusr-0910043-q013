"""项目内部接口说明。"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from jinja2.exceptions import TemplateNotFound

from jupyter_server.base.handlers import FileFindHandler

if TYPE_CHECKING:
    from logging import Logger

    from jinja2 import Template
    from traitlets.config import Config

    from jupyter_server.extension.application import ExtensionApp
    from jupyter_server.serverapp import ServerApp


class ExtensionHandlerJinjaMixin:
    """项目内部接口说明。"""

    def get_template(self, name: str) -> Template:
        """项目内部接口说明。"""
        try:
            env = f"{self.name}_jinja2_env"  # type:ignore[attr-defined]
            template = cast("Template", self.settings[env].get_template(name))  # type:ignore[attr-defined]
            return template
        except TemplateNotFound:
            return cast("Template", super().get_template(name))  # type:ignore[misc]


class ExtensionHandlerMixin:
    """项目内部接口说明。"""

    settings: dict[str, Any]

    def initialize(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.name = name
        try:
            super().initialize(*args, **kwargs)  # type:ignore[misc]
        except TypeError:
            pass

    @property
    def extensionapp(self) -> ExtensionApp:
        return cast("ExtensionApp", self.settings[self.name])

    @property
    def serverapp(self) -> ServerApp:
        key = "serverapp"
        return cast("ServerApp", self.settings[key])

    @property
    def log(self) -> Logger:
        if not hasattr(self, "name"):
            return cast("Logger", super().log)  # type:ignore[misc]
        # Attempt to pull the ExtensionApp's log, otherwise fall back to ServerApp.
        try:
            return cast("Logger", self.extensionapp.log)
        except AttributeError:
            return cast("Logger", self.serverapp.log)

    @property
    def config(self) -> Config:
        return cast("Config", self.settings[f"{self.name}_config"])

    @property
    def server_config(self) -> Config:
        return cast("Config", self.settings["config"])

    @property
    def base_url(self) -> str:
        return cast("str", self.settings.get("base_url", "/"))

    def render_template(self, name: str, **ns) -> str:
        """项目内部接口说明。"""
        template = cast("Template", self.get_template(name))  # type:ignore[attr-defined]
        ns.update(self.template_namespace)  # type:ignore[attr-defined]
        if template.environment is self.settings["jinja2_env"]:
            # default template environment, use default static_url
            ns["static_url"] = super().static_url  # type:ignore[misc]
        return template.render(**ns)

    @property
    def static_url_prefix(self) -> str:
        return self.extensionapp.static_url_prefix

    @property
    def static_path(self) -> str:
        return cast("str", self.settings[f"{self.name}_static_paths"])

    def static_url(self, path: str, include_host: bool | None = None, **kwargs: Any) -> str:
        """项目内部接口说明。"""
        key = f"{self.name}_static_paths"
        try:
            self.require_setting(key, "static_url")  # type:ignore[attr-defined]
        except Exception as e:
            if key in self.settings:
                msg = (
                    "This extension doesn't have any static paths listed. Check that the "
                    "extension's `static_paths` trait is set."
                )
                raise Exception(msg) from None
            else:
                raise e

        get_url = self.settings.get("static_handler_class", FileFindHandler).make_static_url

        if include_host is None:
            include_host = getattr(self, "include_host", False)

        base = ""
        if include_host:
            base = self.request.protocol + "://" + self.request.host  # type:ignore[attr-defined]

        # Hijack settings dict to send extension templates to extension
        # static directory.
        settings = {
            "static_path": self.static_path,
            "static_url_prefix": self.static_url_prefix,
        }

        return base + cast("str", get_url(settings, path, **kwargs))

"""项目内部接口说明。"""

from jupyter_server.services.config.manager import ConfigManager

DEFAULT_SECTION_NAME = "jupyter_server_config"


class ExtensionConfigManager(ConfigManager):
    """项目内部接口说明。"""

    def get_jpserver_extensions(self, section_name=DEFAULT_SECTION_NAME):
        """项目内部接口说明。"""
        data = self.get(section_name)
        return data.get("ServerApp", {}).get("jpserver_extensions", {})

    def enabled(self, name, section_name=DEFAULT_SECTION_NAME, include_root=True):
        """项目内部接口说明。"""
        extensions = self.get_jpserver_extensions(section_name)
        try:
            return extensions[name]
        except KeyError:
            return False

    def enable(self, name):
        """项目内部接口说明。"""
        data = {"ServerApp": {"jpserver_extensions": {name: True}}}
        self.update(name, data)

    def disable(self, name):
        """项目内部接口说明。"""
        data = {"ServerApp": {"jpserver_extensions": {name: False}}}
        self.update(name, data)

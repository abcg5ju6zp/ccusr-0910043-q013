"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os.path
import typing as t

from jupyter_core.paths import jupyter_config_dir, jupyter_config_path
from traitlets import Instance, List, Unicode, default, observe
from traitlets.config import LoggingConfigurable

from jupyter_server.config_manager import BaseJSONConfigManager, recursive_update


class ConfigManager(LoggingConfigurable):
    """项目内部接口说明。"""

    config_dir_name = Unicode("serverconfig", help="""Name of the config directory.""").tag(
        config=True
    )

    # Public API

    def get(self, section_name):
        """项目内部接口说明。"""
        config: dict[str, t.Any] = {}
        # step through back to front, to ensure front of the list is top priority
        for p in self.read_config_path[::-1]:
            cm = BaseJSONConfigManager(config_dir=p)
            recursive_update(config, cm.get(section_name))
        return config

    def set(self, section_name, data):
        """项目内部接口说明。"""
        return self.write_config_manager.set(section_name, data)

    def update(self, section_name, new_data):
        """项目内部接口说明。"""
        return self.write_config_manager.update(section_name, new_data)

    # Private API

    read_config_path = List(Unicode())

    @default("read_config_path")
    def _default_read_config_path(self):
        return [os.path.join(p, self.config_dir_name) for p in jupyter_config_path()]

    write_config_dir = Unicode()

    @default("write_config_dir")
    def _default_write_config_dir(self):
        return os.path.join(jupyter_config_dir(), self.config_dir_name)

    write_config_manager = Instance(BaseJSONConfigManager)

    @default("write_config_manager")
    def _default_write_config_manager(self):
        return BaseJSONConfigManager(config_dir=self.write_config_dir)

    @observe("write_config_dir")
    def _update_write_config_dir(self, change):
        self.write_config_manager = BaseJSONConfigManager(config_dir=self.write_config_dir)

"""项目内部接口说明。"""

from __future__ import annotations

import importlib
import logging
from itertools import starmap

from tornado.gen import multi
from traitlets import Any, Bool, Dict, HasTraits, Instance, List, Unicode, default, observe
from traitlets import validate as validate_trait
from traitlets.config import LoggingConfigurable

from .config import ExtensionConfigManager
from .utils import ExtensionMetadataError, ExtensionModuleNotFound, get_loader, get_metadata


class ExtensionPoint(HasTraits):
    """项目内部接口说明。"""

    _linked = Bool(False)
    _app = Any(None, allow_none=True)

    metadata = Dict()

    log = Instance(logging.Logger)

    @default("log")
    def _default_log(self):
        return logging.getLogger("ExtensionPoint")

    @validate_trait("metadata")
    def _valid_metadata(self, proposed):
        """项目内部接口说明。"""
        metadata = proposed["value"]
        # Verify that the metadata has a "name" key.
        try:
            self._module_name = metadata["module"]
        except KeyError:
            msg = "There is no 'module' key in the extension's metadata packet."
            raise ExtensionMetadataError(msg) from None

        try:
            self._module = importlib.import_module(self._module_name)
        except ImportError:
            msg = (
                f"The submodule '{self._module_name}' could not be found. Are you "
                "sure the extension is installed?"
            )
            raise ExtensionModuleNotFound(msg) from None
        # If the metadata includes an ExtensionApp, create an instance.
        if "app" in metadata:
            self._app = metadata["app"]()
        return metadata

    @property
    def linked(self):
        """项目内部接口说明。"""
        if self.app:
            return self.app._linked
        return self._linked

    @property
    def app(self):
        """项目内部接口说明。"""
        return self._app

    @property
    def config(self):
        """项目内部接口说明。"""
        if self.app:
            return self.app._jupyter_server_config()
        # At some point, we might want to add logic to load config from
        # disk when extensions don't use ExtensionApp.
        else:
            return {}

    @property
    def module_name(self):
        """项目内部接口说明。"""
        return self._module_name

    @property
    def name(self):
        """项目内部接口说明。"""
        if self.app:
            return self.app.name
        return self.metadata.get("name", self.module_name)

    @property
    def module(self):
        """项目内部接口说明。"""
        return self._module

    def _get_linker(self):
        """项目内部接口说明。"""
        if self.app:
            linker = self.app._link_jupyter_server_extension
        else:
            linker = getattr(
                self.module,
                # Search for a _link_jupyter_extension
                "_link_jupyter_server_extension",
                # Otherwise return a dummy function.
                lambda serverapp: None,
            )
        return linker

    def _get_loader(self):
        """项目内部接口说明。"""
        loc = self.app
        if not loc:
            loc = self.module
        loader = get_loader(loc)
        return loader

    def _get_starter(self):
        """项目内部接口说明。"""
        if self.app:
            linker = self.app._start_jupyter_server_extension
        else:

            async def _noop_start(serverapp):
                return

            linker = getattr(
                self.module,
                # Search for a _start_jupyter_extension
                "_start_jupyter_server_extension",
                # Otherwise return a no-op function.
                _noop_start,
            )
        return linker

    def validate(self):
        """项目内部接口说明。"""
        try:
            self._get_linker()
            self._get_loader()
        except Exception:
            return False
        else:
            return True

    def link(self, serverapp):
        """项目内部接口说明。"""
        if not self.linked:
            linker = self._get_linker()
            linker(serverapp)
            # Store this extension as already linked.
            self._linked = True

    def load(self, serverapp):
        """项目内部接口说明。"""
        loader = self._get_loader()
        return loader(serverapp)

    async def start(self, serverapp):
        """项目内部接口说明。"""
        starter = self._get_starter()
        return await starter(serverapp)


class ExtensionPackage(LoggingConfigurable):
    """项目内部接口说明。"""

    name = Unicode(help="Name of the an importable Python package.")
    enabled = Bool(False, help="Whether the extension package is enabled.")

    _linked_points = Dict()
    extension_points = Dict()
    module = Any(allow_none=True, help="The module for this extension package. None if not enabled")
    metadata = List(Dict(), help="Extension metadata loaded from the extension package.")
    version = Unicode(
        help="""
            The version of this extension package, if it can be found.
            Otherwise, an empty string.
            """,
    )

    @default("version")
    def _load_version(self):
        if not self.enabled:
            return ""
        return getattr(self.module, "__version__", "")

    def __init__(self, **kwargs):
        """项目内部接口说明。"""
        super().__init__(**kwargs)
        if self.enabled:
            self._load_metadata()

    def _load_metadata(self):
        """项目内部接口说明。"""
        name = self.name
        try:
            self.module, self.metadata = get_metadata(name, logger=self.log)
        except ImportError as e:
            msg = (
                f"The module '{name}' could not be found ({e}). Are you "
                "sure the extension is installed?"
            )
            raise ExtensionModuleNotFound(msg) from None
        # Create extension point interfaces for each extension path.
        for m in self.metadata:
            point = ExtensionPoint(metadata=m, log=self.log)
            self.extension_points[point.name] = point
        return name

    def validate(self):
        """项目内部接口说明。"""
        return all(extension.validate() for extension in self.extension_points.values())

    def link_point(self, point_name, serverapp):
        """项目内部接口说明。"""
        linked = self._linked_points.get(point_name, False)
        if not linked:
            point = self.extension_points[point_name]
            point.link(serverapp)

    def load_point(self, point_name, serverapp):
        """项目内部接口说明。"""
        point = self.extension_points[point_name]
        return point.load(serverapp)

    async def start_point(self, point_name, serverapp):
        """项目内部接口说明。"""
        point = self.extension_points[point_name]
        return await point.start(serverapp)

    def link_all_points(self, serverapp):
        """项目内部接口说明。"""
        for point_name in self.extension_points:
            self.link_point(point_name, serverapp)

    def load_all_points(self, serverapp):
        """项目内部接口说明。"""
        return [self.load_point(point_name, serverapp) for point_name in self.extension_points]

    async def start_all_points(self, serverapp):
        """项目内部接口说明。"""
        for point_name in self.extension_points:
            await self.start_point(point_name, serverapp)


class ExtensionManager(LoggingConfigurable):
    """项目内部接口说明。"""

    config_manager = Instance(ExtensionConfigManager, allow_none=True)

    serverapp = Any()  # Use Any to avoid circular import of Instance(ServerApp)

    @default("config_manager")
    def _load_default_config_manager(self):
        config_manager = ExtensionConfigManager()
        self._load_config_manager(config_manager)
        return config_manager

    @observe("config_manager")
    def _config_manager_changed(self, change):
        if change.new:
            self._load_config_manager(change.new)

    # The `extensions` attribute provides a dictionary
    # with extension (package) names mapped to their ExtensionPackage interface
    # (see above). This manager simplifies the interaction between the
    # ServerApp and the extensions being appended.
    extensions = Dict(
        help="""
        Dictionary with extension package names as keys
        and ExtensionPackage objects as values.
        """
    )

    @property
    def sorted_extensions(self):
        """项目内部接口说明。"""
        return dict(sorted(self.extensions.items()))

    # The `_linked_extensions` attribute tracks when each extension
    # has been successfully linked to a ServerApp. This helps prevent
    # extensions from being re-linked recursively unintentionally if another
    # extension attempts to link extensions again.
    linked_extensions = Dict(
        help="""
        Dictionary with extension names as keys

        values are True if the extension is linked, False if not.
        """
    )

    @property
    def extension_apps(self):
        """项目内部接口说明。"""
        return {
            name: {point.app for point in extension.extension_points.values() if point.app}
            for name, extension in self.extensions.items()
        }

    @property
    def extension_points(self):
        """项目内部接口说明。"""
        return {
            name: point
            for value in self.extensions.values()
            for name, point in value.extension_points.items()
        }

    def from_config_manager(self, config_manager):
        """项目内部接口说明。"""
        # load triggered via config_manager trait observer
        self.config_manager = config_manager

    def _load_config_manager(self, config_manager):
        """项目内部接口说明。"""
        jpserver_extensions = config_manager.get_jpserver_extensions()
        self.from_jpserver_extensions(jpserver_extensions)

    def from_jpserver_extensions(self, jpserver_extensions):
        """项目内部接口说明。"""
        for name, enabled in jpserver_extensions.items():
            self.add_extension(name, enabled=enabled)

    def add_extension(self, extension_name, enabled=False):
        """项目内部接口说明。"""
        try:
            extpkg = ExtensionPackage(name=extension_name, enabled=enabled)
            self.extensions[extension_name] = extpkg
            return True
        # Raise a warning if the extension cannot be loaded.
        except Exception as e:
            if self.serverapp and self.serverapp.reraise_server_extension_failures:
                raise
            self.log.warning(
                "%s | error adding extension (enabled: %s): %s",
                extension_name,
                enabled,
                e,
                exc_info=True,
            )
        return False

    def link_extension(self, name):
        """项目内部接口说明。"""
        linked = self.linked_extensions.get(name, False)
        extension = self.extensions[name]
        if not linked and extension.enabled:
            try:
                # Link extension and store links
                extension.link_all_points(self.serverapp)
                self.linked_extensions[name] = True
                self.log.info("%s | extension was successfully linked.", name)
            except Exception as e:
                if self.serverapp and self.serverapp.reraise_server_extension_failures:
                    raise
                self.log.warning("%s | error linking extension: %s", name, e, exc_info=True)

    def load_extension(self, name):
        """项目内部接口说明。"""
        extension = self.extensions.get(name)

        if extension and extension.enabled:
            try:
                extension.load_all_points(self.serverapp)
            except Exception as e:
                if self.serverapp and self.serverapp.reraise_server_extension_failures:
                    raise
                self.log.warning(
                    "%s | extension failed loading with message: %r", name, e, exc_info=True
                )
            else:
                self.log.info("%s | extension was successfully loaded.", name)

    async def start_extension(self, name):
        """项目内部接口说明。"""
        extension = self.extensions.get(name)

        if extension and extension.enabled:
            try:
                await extension.start_all_points(self.serverapp)
            except Exception as e:
                if self.serverapp and self.serverapp.reraise_server_extension_failures:
                    raise
                self.log.warning(
                    "%s | extension failed starting with message: %r", name, e, exc_info=True
                )
            else:
                self.log.debug("%s | extension was successfully started.", name)

    async def stop_extension(self, name, apps):
        """项目内部接口说明。"""
        for app in apps:
            self.log.debug("%s | extension app %r stopping", name, app.name)
            await app.stop_extension()
            self.log.debug("%s | extension app %r stopped", name, app.name)

    def link_all_extensions(self):
        """项目内部接口说明。"""
        # Sort the extension names to enforce deterministic linking
        # order.
        for name in self.sorted_extensions:
            self.link_extension(name)

    def load_all_extensions(self):
        """项目内部接口说明。"""
        # Sort the extension names to enforce deterministic loading
        # order.
        for name in self.sorted_extensions:
            self.load_extension(name)

    async def start_all_extensions(self):
        """项目内部接口说明。"""
        # Sort the extension names to enforce deterministic loading
        # order.
        await multi([self.start_extension(name) for name in self.sorted_extensions])

    async def stop_all_extensions(self):
        """项目内部接口说明。"""
        await multi(list(starmap(self.stop_extension, sorted(dict(self.extension_apps).items()))))

    def any_activity(self):
        """项目内部接口说明。"""
        for _, apps in sorted(dict(self.extension_apps).items()):
            for app in apps:
                if app.current_activity():
                    return True

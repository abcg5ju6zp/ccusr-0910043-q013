"""项目内部接口说明。"""

import importlib
import time
import warnings


class ExtensionLoadingError(Exception):
    """项目内部接口说明。"""


class ExtensionMetadataError(Exception):
    """项目内部接口说明。"""


class ExtensionModuleNotFound(Exception):
    """项目内部接口说明。"""


class NotAnExtensionApp(Exception):
    """项目内部接口说明。"""


def get_loader(obj, logger=None):
    """项目内部接口说明。"""
    try:
        return obj._load_jupyter_server_extension
    except AttributeError:
        pass

    try:
        func = obj.load_jupyter_server_extension
    except AttributeError:
        msg = "_load_jupyter_server_extension function was not found."
        raise ExtensionLoadingError(msg) from None

    warnings.warn(
        "A `_load_jupyter_server_extension` function was not "
        f"found in {obj!s}. Instead, a `load_jupyter_server_extension` "
        "function was found and will be used for now. This function "
        "name will be deprecated in future releases "
        "of Jupyter Server.",
        DeprecationWarning,
        stacklevel=2,
    )
    return func


def get_metadata(package_name, logger=None):
    """项目内部接口说明。"""
    start_time = time.perf_counter()
    module = importlib.import_module(package_name)
    end_time = time.perf_counter()
    duration = end_time - start_time
    # Sometimes packages can take a *while* to import, so we report how long
    # each module took to import. This makes it much easier for users to report
    # slow loading modules upstream, as slow loading modules will block server startup
    if logger:
        log = logger.info if duration > 0.1 else logger.debug
        log(f"Extension package {package_name} took {duration:.4f}s to import")

    try:
        return module, module._jupyter_server_extension_points()
    except AttributeError:
        pass

    # For backwards compatibility, we temporarily allow
    # _jupyter_server_extension_paths. We will remove in
    # a later release of Jupyter Server.
    try:
        extension_points = module._jupyter_server_extension_paths()
        if logger:
            logger.warning(
                "A `_jupyter_server_extension_points` function was not "
                f"found in {package_name}. Instead, a `_jupyter_server_extension_paths` "
                "function was found and will be used for now. This function "
                "name will be deprecated in future releases "
                "of Jupyter Server."
            )
        return module, extension_points
    except AttributeError:
        pass

    # Dynamically create metadata if the package doesn't
    # provide it.
    if logger:
        logger.debug(
            "A `_jupyter_server_extension_points` function was "
            f"not found in {package_name}, so Jupyter Server will look "
            "for extension points in the extension pacakge's "
            "root."
        )
    return module, [{"module": package_name, "name": package_name}]


def validate_extension(name):
    """项目内部接口说明。"""
    from .manager import ExtensionPackage

    return ExtensionPackage(name=name)

"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import errno
import importlib.util
import os
import re
import socket
import sys
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, NewType
from urllib.parse import (
    SplitResult,
    quote,
    unquote,
    urlparse,
    urlsplit,
    urlunsplit,
)
from urllib.parse import (
    urljoin as _urljoin,
)
from urllib.request import pathname2url as _pathname2url

from jupyter_core.utils import ensure_async as _ensure_async
from packaging.version import InvalidVersion, Version
from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPRequest, HTTPResponse
from tornado.netutil import Resolver

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

ApiPath = NewType("ApiPath", str)

# Re-export
urljoin = _urljoin
pathname2url = _pathname2url
ensure_async = _ensure_async


def origin_matches_pat(allow_origin_pat: str, origin: str) -> bool:
    """项目内部接口说明。"""
    if not allow_origin_pat:
        return False
    if re.fullmatch(allow_origin_pat, origin):
        return True
    if re.match(allow_origin_pat, origin):
        warnings.warn(
            f"allow_origin_pat {allow_origin_pat!r} only matched the request origin as a prefix. "
            "This has been replaced with a full string match. "
            "Update your pattern if you need to prefix-match the origin (e.g. append '.*')",
            UserWarning,
            stacklevel=3,
        )
    return False


def url_path_join(*pieces: str) -> str:
    """项目内部接口说明。"""
    initial = pieces[0].startswith("/")
    final = pieces[-1].endswith("/")
    stripped = [s.strip("/") for s in pieces]
    result = "/".join(s for s in stripped if s)
    if initial:
        result = "/" + result
    if final:
        result = result + "/"
    if result == "//":
        result = "/"
    return result


def url_is_absolute(url: str) -> bool:
    """项目内部接口说明。"""
    return urlparse(url).path.startswith("/")


def path2url(path: str) -> str:
    """项目内部接口说明。"""
    pieces = [quote(p) for p in path.split(os.sep)]
    # preserve trailing /
    if pieces[-1] == "":
        pieces[-1] = "/"
    url = url_path_join(*pieces)
    return url


def url2path(url: str) -> str:
    """项目内部接口说明。"""
    pieces = [unquote(p) for p in url.split("/")]
    path = os.path.join(*pieces)
    return path


def url_escape(path: str) -> str:
    """项目内部接口说明。"""
    parts = path.split("/")
    return "/".join([quote(p) for p in parts])


def url_unescape(path: str) -> str:
    """项目内部接口说明。"""
    return "/".join([unquote(p) for p in path.split("/")])


def samefile_simple(path: str, other_path: str) -> bool:
    """项目内部接口说明。"""
    path_stat = os.stat(path)
    other_path_stat = os.stat(other_path)
    return path.lower() == other_path.lower() and path_stat == other_path_stat


def to_os_path(path: ApiPath, root: str = "") -> str:
    """项目内部接口说明。"""
    parts = str(path).strip("/").split("/")
    parts = [p for p in parts if p != ""]  #  remove duplicate splits
    path_ = os.path.join(root, *parts)
    return os.path.normpath(path_)


def to_api_path(os_path: str, root: str = "") -> ApiPath:
    """项目内部接口说明。"""
    os_path = os_path.removeprefix(root)
    parts = os_path.strip(os.path.sep).split(os.path.sep)
    parts = [p for p in parts if p != ""]  # remove duplicate splits
    path = "/".join(parts)
    return ApiPath(path)


def check_version(v: str, check: str) -> bool:
    """项目内部接口说明。"""
    try:
        return bool(Version(v) >= Version(check))
    except (TypeError, InvalidVersion):
        # packaging >= 26.3 raises InvalidVersion where it used to raise
        # TypeError for non-string inputs.
        return True


# Copy of IPython.utils.process.check_pid:


def _check_pid_win32(pid: int) -> bool:
    import ctypes

    # OpenProcess returns 0 if no such process (of ours) exists
    # positive int otherwise
    return bool(ctypes.windll.kernel32.OpenProcess(1, 0, pid))  # type:ignore[attr-defined]


def _check_pid_posix(pid: int) -> bool:
    """项目内部接口说明。"""
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        elif err.errno == errno.EPERM:
            # Don't have permission to signal the process - probably means it exists
            return True
        raise
    else:
        return True


if sys.platform == "win32":
    check_pid = _check_pid_win32
else:
    check_pid = _check_pid_posix


async def run_sync_in_loop(maybe_async):
    """项目内部接口说明。"""
    warnings.warn(
        "run_sync_in_loop is deprecated since Jupyter Server 2.0, use 'ensure_async' from jupyter_core instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return ensure_async(maybe_async)


def urlencode_unix_socket_path(socket_path: str) -> str:
    """项目内部接口说明。"""
    return socket_path.replace("/", "%2F")


def urldecode_unix_socket_path(socket_path: str) -> str:
    """项目内部接口说明。"""
    return socket_path.replace("%2F", "/")


def urlencode_unix_socket(socket_path: str) -> str:
    """项目内部接口说明。"""
    return "http+unix://%s" % urlencode_unix_socket_path(socket_path)


def unix_socket_in_use(socket_path: str) -> bool:
    """项目内部接口说明。"""
    if not os.path.exists(socket_path):
        return False

    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(socket_path)
    except OSError:
        return False
    else:
        return True
    finally:
        sock.close()


@contextmanager
def _request_for_tornado_client(
    urlstring: str, method: str = "GET", body: Any = None, headers: Any = None
) -> Generator[HTTPRequest, None, None]:
    """项目内部接口说明。"""
    parts = urlsplit(urlstring)
    if parts.scheme in ["http", "https"]:
        pass
    elif parts.scheme == "http+unix":
        # If unix socket, mimic HTTP.
        parts = SplitResult(
            scheme="http",
            netloc=parts.netloc,
            path=parts.path,
            query=parts.query,
            fragment=parts.fragment,
        )

        class UnixSocketResolver(Resolver):
            """项目内部接口说明。"""

            def initialize(self, resolver):
                self.resolver = resolver

            def close(self):
                self.resolver.close()

            async def resolve(self, host, port, *args, **kwargs):
                return [(socket.AF_UNIX, urldecode_unix_socket_path(host))]

        resolver = UnixSocketResolver(resolver=Resolver())
        AsyncHTTPClient.configure(None, resolver=resolver)
    else:
        msg = "Unknown URL scheme."
        raise Exception(msg)

    # Yield the request for the given client.
    url = urlunsplit(parts)
    request = HTTPRequest(url, method=method, body=body, headers=headers, validate_cert=False)
    yield request


def fetch(
    urlstring: str, method: str = "GET", body: Any = None, headers: Any = None
) -> HTTPResponse:
    """项目内部接口说明。"""
    with _request_for_tornado_client(
        urlstring, method=method, body=body, headers=headers
    ) as request:
        response = HTTPClient(AsyncHTTPClient).fetch(request)
    return response


async def async_fetch(
    urlstring: str, method: str = "GET", body: Any = None, headers: Any = None, io_loop: Any = None
) -> HTTPResponse:
    """项目内部接口说明。"""
    with _request_for_tornado_client(
        urlstring, method=method, body=body, headers=headers
    ) as request:
        response = await AsyncHTTPClient(io_loop).fetch(request)
    return response


def is_namespace_package(namespace: str) -> bool | None:
    """项目内部接口说明。"""
    # NOTE: using submodule_search_locations because the loader can be None
    try:
        spec = importlib.util.find_spec(namespace)
    except ValueError:  # spec is not set - see https://docs.python.org/3/library/importlib.html#importlib.util.find_spec
        return None

    if not spec:
        # e.g. module not installed
        return None
    return bool(spec.origin is None and spec.submodule_search_locations)


def filefind(filename: str, path_dirs: Sequence[str]) -> str:
    """项目内部接口说明。"""
    file_path = Path(filename)

    # If the input is an absolute path, reject it
    if file_path.is_absolute():
        msg = f"{filename} is absolute, filefind only accepts relative paths."
        raise OSError(msg)

    for path_str in path_dirs:
        path = Path(path_str).absolute()
        test_path = path / file_path
        # os.path.abspath resolves '..', but Path.absolute() doesn't
        # Path.resolve() does, but traverses symlinks, which we don't want
        test_path = Path(os.path.abspath(test_path))
        if not test_path.is_relative_to(path):
            # points outside root, e.g. via `filename='../foo'`
            continue
        # make sure we don't call is_file before we know it's a file within a prefix
        # GHSA-hrw6-wg82-cm62 - can leak password hash on windows.
        if test_path.is_file():
            return os.path.abspath(test_path)

    msg = f"File {filename!r} does not exist in any of the search paths: {path_dirs!r}"
    raise OSError(msg)


def import_item(name: str) -> Any:
    """项目内部接口说明。"""

    parts = name.rsplit(".", 1)
    if len(parts) == 2:
        # called with 'foo.bar....'
        package, obj = parts
        module = __import__(package, fromlist=[obj])
        try:
            pak = getattr(module, obj)
        except AttributeError as e:
            raise ImportError("No module named %s" % obj) from e
        return pak
    else:
        # called with un-dotted string
        return __import__(parts[0])


class JupyterServerAuthWarning(RuntimeWarning):
    """项目内部接口说明。"""

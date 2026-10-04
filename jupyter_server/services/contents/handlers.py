"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import json
from http import HTTPStatus
from typing import Any

try:
    from jupyter_client.jsonutil import json_default
except ImportError:
    from jupyter_client.jsonutil import date_default as json_default

from jupyter_core.utils import ensure_async
from tornado import web

from jupyter_server.auth.decorator import allow_unauthenticated, authorized
from jupyter_server.base.handlers import APIHandler, JupyterHandler, path_regex
from jupyter_server.utils import url_escape, url_path_join

AUTH_RESOURCE = "contents"


def _validate_keys(expect_defined: bool, model: dict[str, Any], keys: list[str]):
    """项目内部接口说明。"""

    if expect_defined:
        errors = [key for key in keys if model[key] is None]
        if errors:
            raise web.HTTPError(
                500,
                f"Keys unexpectedly None: {errors}",
            )
    else:
        errors = {key: model[key] for key in keys if model[key] is not None}  # type: ignore[assignment]
        if errors:
            raise web.HTTPError(
                500,
                f"Keys unexpectedly not None: {errors}",
            )


def validate_model(model, expect_content=False, expect_hash=False):
    """项目内部接口说明。"""
    required_keys = {
        "name",
        "path",
        "type",
        "writable",
        "created",
        "last_modified",
        "mimetype",
        "content",
        "format",
    }
    if expect_hash:
        required_keys.update(["hash", "hash_algorithm"])
    missing = required_keys - set(model.keys())
    if missing:
        raise web.HTTPError(
            500,
            f"Missing Model Keys: {missing}",
        )

    content_keys = ["content", "format"]
    _validate_keys(expect_content, model, content_keys)
    if expect_hash:
        _validate_keys(expect_hash, model, ["hash", "hash_algorithm"])


class ContentsAPIHandler(APIHandler):
    """项目内部接口说明。"""

    auth_resource = AUTH_RESOURCE


class ContentsHandler(ContentsAPIHandler):
    """项目内部接口说明。"""

    def location_url(self, path):
        """项目内部接口说明。"""
        return url_path_join(self.base_url, "api", "contents", url_escape(path))

    def _finish_model(self, model, location=True):
        """项目内部接口说明。"""
        if location:
            location = self.location_url(model["path"])
            self.set_header("Location", location)
        self.set_header("Last-Modified", model["last_modified"])
        self.set_header("Content-Type", "application/json")
        self.finish(json.dumps(model, default=json_default))

    async def _finish_error(self, code, message):
        """项目内部接口说明。"""
        self.set_status(code)
        self.write(message)
        await self.finish()

    @web.authenticated
    @authorized
    async def get(self, path=""):
        """项目内部接口说明。"""
        path = path or ""
        cm = self.contents_manager

        type = self.get_query_argument("type", default=None)
        if type not in {None, "directory", "file", "notebook"}:
            # fall back to file if unknown type
            type = "file"

        format = self.get_query_argument("format", default=None)
        if format not in {None, "text", "base64"}:
            raise web.HTTPError(400, "Format %r is invalid" % format)
        content_str = self.get_query_argument("content", default="1")
        if content_str not in {"0", "1"}:
            raise web.HTTPError(400, "Content %r is invalid" % content_str)
        content = int(content_str or "")

        hash_str = self.get_query_argument("hash", default="0")
        if hash_str not in {"0", "1"}:
            raise web.HTTPError(
                400, f"Hash argument {hash_str!r} is invalid. It must be '0' or '1'."
            )
        require_hash = int(hash_str)

        if not cm.allow_hidden and await ensure_async(cm.is_hidden(path)):
            await self._finish_error(
                HTTPStatus.NOT_FOUND, f"file or directory {path!r} does not exist"
            )
            return

        try:
            expect_hash = require_hash
            try:
                model = await ensure_async(
                    self.contents_manager.get(
                        path=path,
                        type=type,
                        format=format,
                        content=content,
                        require_hash=require_hash,
                    )
                )
            except TypeError:
                # Fallback for ContentsManager not handling the require_hash argument
                # introduced in 2.11
                expect_hash = False
                model = await ensure_async(
                    self.contents_manager.get(
                        path=path,
                        type=type,
                        format=format,
                        content=content,
                    )
                )
            validate_model(model, expect_content=content, expect_hash=expect_hash)
            self._finish_model(model, location=False)
        except web.HTTPError as exc:
            # 404 is okay in this context, catch exception and return 404 code to prevent stack trace on client
            if exc.status_code == HTTPStatus.NOT_FOUND:
                await self._finish_error(
                    HTTPStatus.NOT_FOUND, f"file or directory {path!r} does not exist"
                )
            raise

    @web.authenticated
    @authorized
    async def patch(self, path=""):
        """项目内部接口说明。"""
        cm = self.contents_manager
        model = self.get_json_body()
        if model is None:
            raise web.HTTPError(400, "JSON body missing")

        old_path = model.get("path")
        if (
            old_path
            and not cm.allow_hidden
            and (
                await ensure_async(cm.is_hidden(path)) or await ensure_async(cm.is_hidden(old_path))
            )
        ):
            raise web.HTTPError(400, f"Cannot rename file or directory {path!r}")

        model = await ensure_async(cm.update(model, path))
        validate_model(model)
        self._finish_model(model)

    async def _copy(self, copy_from, copy_to=None):
        """项目内部接口说明。"""
        self.log.info(
            "Copying %r to %r",
            copy_from,
            copy_to or "",
        )
        model = await ensure_async(self.contents_manager.copy(copy_from, copy_to))
        self.set_status(201)
        validate_model(model)
        self._finish_model(model)

    async def _upload(self, model, path):
        """项目内部接口说明。"""
        self.log.info("Uploading file to %s", path)
        model = await ensure_async(self.contents_manager.new(model, path))
        self.set_status(201)
        validate_model(model)
        self._finish_model(model)

    async def _new_untitled(self, path, type="", ext=""):
        """项目内部接口说明。"""
        self.log.info("Creating new %s in %s", type or "file", path)
        model = await ensure_async(
            self.contents_manager.new_untitled(path=path, type=type, ext=ext)
        )
        self.set_status(201)
        validate_model(model)
        self._finish_model(model)

    async def _save(self, model, path):
        """项目内部接口说明。"""
        chunk = model.get("chunk", None)
        if not chunk or chunk == -1:  # Avoid tedious log information
            self.log.info("Saving file at %s", path)
        model = await ensure_async(self.contents_manager.save(model, path))
        validate_model(model)
        self._finish_model(model)

    @web.authenticated
    @authorized
    async def post(self, path=""):
        """项目内部接口说明。"""

        cm = self.contents_manager

        file_exists = await ensure_async(cm.file_exists(path))
        if file_exists:
            raise web.HTTPError(400, "Cannot POST to files, use PUT instead.")

        model = self.get_json_body()
        if model:
            copy_from = model.get("copy_from")
            if copy_from:
                if not cm.allow_hidden and (
                    await ensure_async(cm.is_hidden(path))
                    or await ensure_async(cm.is_hidden(copy_from))
                ):
                    raise web.HTTPError(400, f"Cannot copy file or directory {path!r}")
                else:
                    await self._copy(copy_from, path)
            else:
                ext = model.get("ext", "")
                type = model.get("type", "")
                if type not in {None, "", "directory", "file", "notebook"}:
                    # fall back to file if unknown type
                    type = "file"
                await self._new_untitled(path, type=type, ext=ext)
        else:
            await self._new_untitled(path)

    @web.authenticated
    @authorized
    async def put(self, path=""):
        """项目内部接口说明。"""
        model = self.get_json_body()
        cm = self.contents_manager

        if model:
            if model.get("copy_from"):
                raise web.HTTPError(400, "Cannot copy with PUT, only POST")
            if not cm.allow_hidden and (
                (model.get("path") and await ensure_async(cm.is_hidden(model.get("path"))))
                or await ensure_async(cm.is_hidden(path))
            ):
                raise web.HTTPError(400, f"Cannot create file or directory {path!r}")

            exists = await ensure_async(self.contents_manager.file_exists(path))
            if model.get("type", "") not in {None, "", "directory", "file", "notebook"}:
                # fall back to file if unknown type
                model["type"] = "file"
            if exists:
                await self._save(model, path)
            else:
                await self._upload(model, path)
        else:
            await self._new_untitled(path)

    @web.authenticated
    @authorized
    async def delete(self, path=""):
        """项目内部接口说明。"""
        cm = self.contents_manager

        if not cm.allow_hidden and await ensure_async(cm.is_hidden(path)):
            raise web.HTTPError(400, f"Cannot delete file or directory {path!r}")

        self.log.warning("delete %s", path)
        await ensure_async(cm.delete(path))
        self.set_status(204)
        self.finish()


class CheckpointsHandler(ContentsAPIHandler):
    """项目内部接口说明。"""

    @web.authenticated
    @authorized
    async def get(self, path=""):
        """项目内部接口说明。"""
        cm = self.contents_manager
        checkpoints = await ensure_async(cm.list_checkpoints(path))
        data = json.dumps(checkpoints, default=json_default)
        self.finish(data)

    @web.authenticated
    @authorized
    async def post(self, path=""):
        """项目内部接口说明。"""
        cm = self.contents_manager
        checkpoint = await ensure_async(cm.create_checkpoint(path))
        data = json.dumps(checkpoint, default=json_default)
        location = url_path_join(
            self.base_url,
            "api/contents",
            url_escape(path),
            "checkpoints",
            url_escape(checkpoint["id"]),
        )
        self.set_header("Location", location)
        self.set_status(201)
        self.finish(data)


class ModifyCheckpointsHandler(ContentsAPIHandler):
    """项目内部接口说明。"""

    @web.authenticated
    @authorized
    async def post(self, path, checkpoint_id):
        """项目内部接口说明。"""
        cm = self.contents_manager
        await ensure_async(cm.restore_checkpoint(checkpoint_id, path))
        self.set_status(204)
        self.finish()

    @web.authenticated
    @authorized
    async def delete(self, path, checkpoint_id):
        """项目内部接口说明。"""
        cm = self.contents_manager
        await ensure_async(cm.delete_checkpoint(checkpoint_id, path))
        self.set_status(204)
        self.finish()


class NotebooksRedirectHandler(JupyterHandler):
    """项目内部接口说明。"""

    SUPPORTED_METHODS = (
        "GET",
        "PUT",
        "PATCH",
        "POST",
        "DELETE",
    )

    @allow_unauthenticated
    def get(self, path):
        """项目内部接口说明。"""
        self.log.warning("/api/notebooks is deprecated, use /api/contents")
        self.redirect(url_path_join(self.base_url, "api/contents", url_escape(path)))

    put = patch = post = delete = get


class TrustNotebooksHandler(JupyterHandler):
    """项目内部接口说明。"""

    @web.authenticated
    @authorized(resource=AUTH_RESOURCE)
    async def post(self, path=""):
        """项目内部接口说明。"""
        cm = self.contents_manager
        await ensure_async(cm.trust_notebook(path))
        self.set_status(201)
        self.finish()


# -----------------------------------------------------------------------------
# URL to handler mappings
# -----------------------------------------------------------------------------


_checkpoint_id_regex = r"(?P<checkpoint_id>[\w-]+)"


default_handlers = [
    (r"/api/contents%s/checkpoints" % path_regex, CheckpointsHandler),
    (
        rf"/api/contents{path_regex}/checkpoints/{_checkpoint_id_regex}",
        ModifyCheckpointsHandler,
    ),
    (r"/api/contents%s/trust" % path_regex, TrustNotebooksHandler),
    (r"/api/contents%s" % path_regex, ContentsHandler),
    (r"/api/notebooks/?(.*)", NotebooksRedirectHandler),
]

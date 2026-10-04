"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from tornado.web import HTTPError
from traitlets.config.configurable import LoggingConfigurable


class Checkpoints(LoggingConfigurable):
    """项目内部接口说明。"""

    def create_checkpoint(self, contents_mgr, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        """项目内部接口说明。"""
        raise NotImplementedError

    def delete_checkpoint(self, checkpoint_id, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    def list_checkpoints(self, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    def rename_all_checkpoints(self, old_path, new_path):
        """项目内部接口说明。"""
        for cp in self.list_checkpoints(old_path):
            self.rename_checkpoint(cp["id"], old_path, new_path)

    def delete_all_checkpoints(self, path):
        """项目内部接口说明。"""
        for checkpoint in self.list_checkpoints(path):
            self.delete_checkpoint(checkpoint["id"], path)


class GenericCheckpointsMixin:
    """项目内部接口说明。"""

    def create_checkpoint(self, contents_mgr, path):
        model = contents_mgr.get(path, content=True)
        type_ = model["type"]
        if type_ == "notebook":
            return self.create_notebook_checkpoint(
                model["content"],
                path,
            )
        elif type_ == "file":
            return self.create_file_checkpoint(
                model["content"],
                model["format"],
                path,
            )
        else:
            raise HTTPError(500, "Unexpected type %s" % type)

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        """项目内部接口说明。"""
        type_ = contents_mgr.get(path, content=False)["type"]
        if type_ == "notebook":
            model = self.get_notebook_checkpoint(checkpoint_id, path)
        elif type_ == "file":
            model = self.get_file_checkpoint(checkpoint_id, path)
        else:
            raise HTTPError(500, "Unexpected type %s" % type_)
        contents_mgr.save(model, path)

    # Required Methods
    def create_file_checkpoint(self, content, format, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    def create_notebook_checkpoint(self, nb, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    def get_file_checkpoint(self, checkpoint_id, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    def get_notebook_checkpoint(self, checkpoint_id, path):
        """项目内部接口说明。"""
        raise NotImplementedError


class AsyncCheckpoints(Checkpoints):
    """项目内部接口说明。"""

    async def create_checkpoint(self, contents_mgr, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    async def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    async def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        """项目内部接口说明。"""
        raise NotImplementedError

    async def delete_checkpoint(self, checkpoint_id, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    async def list_checkpoints(self, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    async def rename_all_checkpoints(self, old_path, new_path):
        """项目内部接口说明。"""
        for cp in await self.list_checkpoints(old_path):
            await self.rename_checkpoint(cp["id"], old_path, new_path)

    async def delete_all_checkpoints(self, path):
        """项目内部接口说明。"""
        for checkpoint in await self.list_checkpoints(path):
            await self.delete_checkpoint(checkpoint["id"], path)


class AsyncGenericCheckpointsMixin(GenericCheckpointsMixin):
    """项目内部接口说明。"""

    async def create_checkpoint(self, contents_mgr, path):
        model = await contents_mgr.get(path, content=True)
        type_ = model["type"]
        if type_ == "notebook":
            return await self.create_notebook_checkpoint(
                model["content"],
                path,
            )
        elif type_ == "file":
            return await self.create_file_checkpoint(
                model["content"],
                model["format"],
                path,
            )
        else:
            raise HTTPError(500, "Unexpected type %s" % type_)

    async def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        """项目内部接口说明。"""
        content_model = await contents_mgr.get(path, content=False)
        type_ = content_model["type"]
        if type_ == "notebook":
            model = await self.get_notebook_checkpoint(checkpoint_id, path)
        elif type_ == "file":
            model = await self.get_file_checkpoint(checkpoint_id, path)
        else:
            raise HTTPError(500, "Unexpected type %s" % type_)
        await contents_mgr.save(model, path)

    # Required Methods
    async def create_file_checkpoint(self, content, format, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    async def create_notebook_checkpoint(self, nb, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    async def get_file_checkpoint(self, checkpoint_id, path):
        """项目内部接口说明。"""
        raise NotImplementedError

    async def get_notebook_checkpoint(self, checkpoint_id, path):
        """项目内部接口说明。"""
        raise NotImplementedError

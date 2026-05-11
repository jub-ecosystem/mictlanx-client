from __future__ import annotations
from typing import Any, Dict, List

from option import Result, Ok, Err
from mictlanx.apac.backends.base import AbstractStorageBackend


class DropboxBackend(AbstractStorageBackend):
    """
    Storage backend backed by Dropbox.

    Requires: dropbox (`pip install dropbox`)
    Objects are stored at /<root_path>/<bucket_id>/<key>.
    """

    def __init__(
        self,
        backend_id: str,
        access_token: str,
        root_path: str = "/mictlanx",
    ) -> None:
        self._id = backend_id
        self._root = "/" + root_path.strip("/")

        try:
            import dropbox as dbx_lib
            self._dbx = dbx_lib.Dropbox(access_token)
        except ImportError:
            raise ImportError("Dropbox SDK required: pip install dropbox")

    @property
    def backend_id(self) -> str:
        return self._id

    def _path(self, bucket_id: str, key: str) -> str:
        return f"{self._root}/{bucket_id}/{key}"

    async def put(
        self,
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str] = {},
        content_type: str = "application/octet-stream",
    ) -> Result[Dict[str, Any], Exception]:
        try:
            import dropbox
            path = self._path(bucket_id, key)
            result = self._dbx.files_upload(
                value,
                path,
                mode=dropbox.files.WriteMode.overwrite,
            )
            return Ok({"path": result.path_display, "size": result.size})
        except Exception as e:
            return Err(e)

    async def get(self, bucket_id: str, key: str) -> Result[bytes, Exception]:
        try:
            path = self._path(bucket_id, key)
            _, response = self._dbx.files_download(path)
            return Ok(response.content)
        except Exception as e:
            return Err(e)

    async def list(self, bucket_id: str) -> Result[List[str], Exception]:
        try:
            folder_path = f"{self._root}/{bucket_id}"
            result = self._dbx.files_list_folder(folder_path)
            keys = [entry.name for entry in result.entries]
            while result.has_more:
                result = self._dbx.files_list_folder_continue(result.cursor)
                keys.extend(entry.name for entry in result.entries)
            return Ok(keys)
        except Exception as e:
            return Err(e)

    async def delete(self, bucket_id: str, key: str) -> Result[bool, Exception]:
        try:
            path = self._path(bucket_id, key)
            self._dbx.files_delete_v2(path)
            return Ok(True)
        except Exception as e:
            return Err(e)

    async def health(self) -> bool:
        try:
            self._dbx.users_get_current_account()
            return True
        except Exception:
            return False

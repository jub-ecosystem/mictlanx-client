from __future__ import annotations
from typing import Any, Dict, List
import hashlib

from option import Result, Ok, Err
from mictlanx.apac.backends.base import AbstractStorageBackend
from mictlanx.services import AsyncPeer


class MictlanXBackend(AbstractStorageBackend):
    """
    Storage backend backed by a single MictlanX AsyncPeer.

    PUT uses the two-step protocol: put_metadata → put_data.
    GET assembles bytes from get_streaming.
    """

    def __init__(self, peer: AsyncPeer) -> None:
        self._peer = peer
        self._id = f"mictlanx-{peer.peer_id}"

    @property
    def backend_id(self) -> str:
        return self._id

    async def put(
        self,
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str] = {},
        content_type: str = "application/octet-stream",
    ) -> Result[Dict[str, Any], Exception]:
        checksum = hashlib.sha256(value).hexdigest()
        size = len(value)

        meta_result = await self._peer.put_metadata(
            key=key,
            size=size,
            checksum=checksum,
            producer_id="apac",
            content_type=content_type,
            ball_id=key,
            bucket_id=bucket_id,
            tags=tags,
        )
        if meta_result.is_err:
            return Err(meta_result.unwrap_err())

        task_id = meta_result.unwrap().task_id
        data_result = await self._peer.put_data(
            task_id=task_id,
            key=key,
            value=value,
            content_type=content_type,
        )
        if data_result.is_err:
            return Err(data_result.unwrap_err())

        return Ok({"task_id": task_id, "checksum": checksum, "size": size})

    async def get(self, bucket_id: str, key: str) -> Result[bytes, Exception]:
        result = await self._peer.get_streaming(bucket_id=bucket_id, key=key)
        if result.is_err:
            return Err(result.unwrap_err())
        response = result.unwrap()
        data: bytes = response.data.tobytes() if hasattr(response.data, "tobytes") else bytes(response.data)
        return Ok(data)

    async def list(self, bucket_id: str) -> Result[List[str], Exception]:
        result = await self._peer.get_bucket_metadata(bucket_id=bucket_id)
        if result.is_err:
            return Err(result.unwrap_err())
        balls = result.unwrap()
        keys = [b.key for b in balls] if hasattr(balls, "__iter__") else []
        return Ok(keys)

    async def delete(self, bucket_id: str, key: str) -> Result[bool, Exception]:
        result = await self._peer.delete(bucket_id=bucket_id, key=key)
        if result.is_err:
            return Err(result.unwrap_err())
        return Ok(True)

    async def health(self) -> bool:
        try:
            result = await self._peer.health()
            return result.is_ok
        except Exception:
            return False

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from option import Result


class AbstractStorageBackend(ABC):
    """
    Adapter for a single storage endpoint.

    One concrete instance = one logical endpoint (a peer, an S3 bucket/prefix,
    a Drive folder, a Dropbox path, etc.).  The replication layer calls these
    methods directly; implementations must not know about replication.
    """

    @property
    @abstractmethod
    def backend_id(self) -> str:
        """Unique identifier for this backend instance (used in replica maps and metrics)."""
        ...

    @abstractmethod
    async def put(
        self,
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str] = {},
        content_type: str = "application/octet-stream",
    ) -> Result[Dict[str, Any], Exception]:
        """Store value under (bucket_id, key). Returns metadata dict on success."""
        ...

    @abstractmethod
    async def get(self, bucket_id: str, key: str) -> Result[bytes, Exception]:
        """Retrieve the raw bytes stored under (bucket_id, key)."""
        ...

    @abstractmethod
    async def list(self, bucket_id: str) -> Result[List[str], Exception]:
        """Return a list of keys in the given bucket."""
        ...

    @abstractmethod
    async def delete(self, bucket_id: str, key: str) -> Result[bool, Exception]:
        """Delete the object at (bucket_id, key). Returns True if deleted."""
        ...

    @abstractmethod
    async def health(self) -> bool:
        """Return True if the backend is reachable and healthy."""
        ...

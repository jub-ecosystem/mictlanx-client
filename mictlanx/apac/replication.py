from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import asyncio
import time

from mictlanx.apac.events import EventBus, EventType, StorageEvent


@dataclass
class ReplicaOutcome:
    backend_id: str
    success: bool
    latency_ms: float
    error: Optional[Exception] = None


@dataclass
class ReplicationResult:
    bucket_id: str
    key: str
    total: int
    succeeded: int
    replicas: List[ReplicaOutcome] = field(default_factory=list)
    first_available_backend_id: Optional[str] = None

    @property
    def failed(self) -> int:
        return self.total - self.succeeded

    @property
    def all_ok(self) -> bool:
        return self.succeeded == self.total

    @property
    def is_ok(self) -> bool:
        return self.succeeded > 0

    def successful_backend_ids(self) -> List[str]:
        return [o.backend_id for o in self.replicas if o.success]


class AbstractReplicationStrategy(ABC):
    @abstractmethod
    async def replicate(
        self,
        backends: List[Any],  # List[AbstractStorageBackend] — forward ref avoided
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str],
        content_type: str,
        bus: EventBus,
    ) -> ReplicationResult: ...


class ActiveReplicationStrategy(AbstractReplicationStrategy):
    """
    Uploads to all backends in parallel.

    Availability contract: REPLICA_AVAILABLE is emitted only after every backend
    returns OK (REPLICATION_COMPLETE).  If any backend fails, the write is
    considered incomplete and no availability event is raised.
    """

    async def replicate(
        self,
        backends: List[Any],
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str],
        content_type: str,
        bus: EventBus,
    ) -> ReplicationResult:
        outcomes: List[ReplicaOutcome] = await asyncio.gather(
            *[self._write(b, bucket_id, key, value, tags, content_type, bus) for b in backends]
        )
        succeeded = sum(1 for o in outcomes if o.success)
        all_ok = succeeded == len(backends)
        first = backends[0].backend_id if all_ok else None

        if all_ok:
            bus.emit(StorageEvent(
                event_type=EventType.REPLICATION_COMPLETE,
                backend_id="*",
                bucket_id=bucket_id,
                key=key,
                meta={"strategy": "active", "replicas": succeeded},
            ))
            bus.emit(StorageEvent(
                event_type=EventType.REPLICA_AVAILABLE,
                backend_id=first or "",
                bucket_id=bucket_id,
                key=key,
                meta={"strategy": "active"},
            ))

        return ReplicationResult(
            bucket_id=bucket_id,
            key=key,
            total=len(backends),
            succeeded=succeeded,
            replicas=outcomes,
            first_available_backend_id=first,
        )

    async def _write(
        self,
        backend: Any,
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str],
        content_type: str,
        bus: EventBus,
    ) -> ReplicaOutcome:
        bus.emit(StorageEvent(EventType.PUT_STARTED, backend.backend_id, bucket_id, key))
        start = time.time()
        result = await backend.put(bucket_id, key, value, tags, content_type)
        latency_ms = (time.time() - start) * 1000

        if result.is_ok:
            bus.emit(StorageEvent(
                event_type=EventType.PUT_COMPLETED,
                backend_id=backend.backend_id,
                bucket_id=bucket_id,
                key=key,
                size_bytes=len(value),
                latency_ms=latency_ms,
                success=True,
            ))
            return ReplicaOutcome(backend_id=backend.backend_id, success=True, latency_ms=latency_ms)

        err = result.unwrap_err()
        bus.emit(StorageEvent(
            event_type=EventType.PUT_FAILED,
            backend_id=backend.backend_id,
            bucket_id=bucket_id,
            key=key,
            latency_ms=latency_ms,
            success=False,
            meta={"error": str(err)},
        ))
        return ReplicaOutcome(backend_id=backend.backend_id, success=False, latency_ms=latency_ms, error=err)


class PassiveReplicationStrategy(AbstractReplicationStrategy):
    """
    Uploads to all backends in parallel.

    Availability contract: REPLICA_AVAILABLE fires as soon as the FIRST backend
    returns OK.  The remaining uploads continue; REPLICATION_COMPLETE fires once
    every backend has responded (success or failure).
    """

    async def replicate(
        self,
        backends: List[Any],
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str],
        content_type: str,
        bus: EventBus,
    ) -> ReplicationResult:
        outcomes: List[Optional[ReplicaOutcome]] = [None] * len(backends)
        lock = asyncio.Lock()
        first_available: Optional[str] = None
        first_emitted = False

        async def write_one(i: int, backend: Any) -> None:
            nonlocal first_available, first_emitted
            bus.emit(StorageEvent(EventType.PUT_STARTED, backend.backend_id, bucket_id, key))
            start = time.time()
            result = await backend.put(bucket_id, key, value, tags, content_type)
            latency_ms = (time.time() - start) * 1000

            if result.is_ok:
                bus.emit(StorageEvent(
                    event_type=EventType.PUT_COMPLETED,
                    backend_id=backend.backend_id,
                    bucket_id=bucket_id,
                    key=key,
                    size_bytes=len(value),
                    latency_ms=latency_ms,
                    success=True,
                ))
                async with lock:
                    if not first_emitted:
                        first_available = backend.backend_id
                        first_emitted = True
                        bus.emit(StorageEvent(
                            event_type=EventType.REPLICA_AVAILABLE,
                            backend_id=backend.backend_id,
                            bucket_id=bucket_id,
                            key=key,
                            meta={"strategy": "passive"},
                        ))
                outcomes[i] = ReplicaOutcome(backend_id=backend.backend_id, success=True, latency_ms=latency_ms)
            else:
                err = result.unwrap_err()
                bus.emit(StorageEvent(
                    event_type=EventType.PUT_FAILED,
                    backend_id=backend.backend_id,
                    bucket_id=bucket_id,
                    key=key,
                    latency_ms=latency_ms,
                    success=False,
                    meta={"error": str(err)},
                ))
                outcomes[i] = ReplicaOutcome(backend_id=backend.backend_id, success=False, latency_ms=latency_ms, error=err)

        await asyncio.gather(*[write_one(i, b) for i, b in enumerate(backends)])

        final = [o for o in outcomes if o is not None]
        succeeded = sum(1 for o in final if o.success)

        if succeeded == len(backends):
            bus.emit(StorageEvent(
                event_type=EventType.REPLICATION_COMPLETE,
                backend_id="*",
                bucket_id=bucket_id,
                key=key,
                meta={"strategy": "passive", "replicas": succeeded},
            ))

        return ReplicationResult(
            bucket_id=bucket_id,
            key=key,
            total=len(backends),
            succeeded=succeeded,
            replicas=final,
            first_available_backend_id=first_available,
        )

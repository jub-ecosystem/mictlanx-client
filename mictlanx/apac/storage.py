from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import asyncio

from option import Result, Ok, Err
from mictlanx.apac.backends.base import AbstractStorageBackend
from mictlanx.apac.events import EventBus, EventType, StorageEvent
from mictlanx.apac.metrics import MetricsCollector
from mictlanx.apac.replication import (
    AbstractReplicationStrategy,
    ActiveReplicationStrategy,
    PassiveReplicationStrategy,
    ReplicationResult,
)
from mictlanx.apac.controller import APaCController
import time


class AbstractStorageSystem(ABC):
    """
    Orchestrates reads, writes, and deletes across a pool of AbstractStorageBackend
    instances with a pluggable replication strategy and APaC-driven routing.

    Subclasses choose a replication strategy (active vs passive) and optionally
    attach an AbstractAPaCStrategy for elastic scaling.
    """

    @abstractmethod
    async def put(
        self,
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str] = {},
        content_type: str = "application/octet-stream",
    ) -> Result[ReplicationResult, Exception]: ...

    @abstractmethod
    async def get(self, bucket_id: str, key: str) -> Result[bytes, Exception]: ...

    @abstractmethod
    async def list(self, bucket_id: str) -> Result[List[str], Exception]: ...

    @abstractmethod
    async def delete(self, bucket_id: str, key: str) -> Result[bool, Exception]: ...

    @abstractmethod
    def add_backend(self, backend: AbstractStorageBackend) -> None: ...

    @abstractmethod
    def remove_backend(self, backend_id: str) -> None: ...


class _ReplicatedStorageSystem(AbstractStorageSystem):
    """
    Base implementation shared by all concrete storage systems.

    Responsibilities:
    - Maintains a replica map: (bucket_id, key) → [backend_id, ...]
    - Routes reads through either the APaCStrategy or APaCController (strategy takes precedence)
    - Emits queue events around each backend call
    - After each successful GET:
        * If an APaCStrategy is set  → runs _handle_scale() in a background task
        * Else if a controller is set → runs controller.check_and_replicate() in a background task
    """

    def __init__(
        self,
        backends: List[AbstractStorageBackend],
        replication_strategy: AbstractReplicationStrategy,
        controller: Optional[APaCController] = None,
        apac_strategy: Optional[Any] = None,   # AbstractAPaCStrategy
        bus: Optional[EventBus] = None,
    ) -> None:
        self._backends: Dict[str, AbstractStorageBackend] = {b.backend_id: b for b in backends}
        self._replication_strategy = replication_strategy
        self._bus = bus or EventBus()
        self._metrics = MetricsCollector(self._bus)
        self._controller = controller
        self._apac_strategy = apac_strategy
        self._replica_map: Dict[str, List[str]] = {}  # f"{bucket_id}\x00{key}" → [backend_id]

    # ---- public helpers ----

    def add_backend(self, backend: AbstractStorageBackend) -> None:
        self._backends[backend.backend_id] = backend

    def remove_backend(self, backend_id: str) -> None:
        self._backends.pop(backend_id, None)

    @property
    def metrics(self) -> MetricsCollector:
        return self._metrics

    @property
    def bus(self) -> EventBus:
        return self._bus

    def _rkey(self, bucket_id: str, key: str) -> str:
        return f"{bucket_id}\x00{key}"

    def _backends_holding(self, bucket_id: str, key: str) -> List[AbstractStorageBackend]:
        ids = self._replica_map.get(self._rkey(bucket_id, key), [])
        return [self._backends[bid] for bid in ids if bid in self._backends]

    # ---- put ----

    async def put(
        self,
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str] = {},
        content_type: str = "application/octet-stream",
    ) -> Result[ReplicationResult, Exception]:
        backends = list(self._backends.values())
        if not backends:
            return Err(Exception("No backends configured"))

        strategy = (
            self._apac_strategy.replication_strategy
            if self._apac_strategy else self._replication_strategy
        )
        result = await strategy.replicate(
            backends=backends,
            bucket_id=bucket_id,
            key=key,
            value=value,
            tags=tags,
            content_type=content_type,
            bus=self._bus,
        )
        if result.is_ok:
            rkey = self._rkey(bucket_id, key)
            self._replica_map[rkey] = result.successful_backend_ids()
        return Ok(result)

    # ---- get ----

    async def get(self, bucket_id: str, key: str) -> Result[bytes, Exception]:
        holders = self._backends_holding(bucket_id, key)
        available_ids = [b.backend_id for b in holders] or list(self._backends.keys())

        # Routing: APaC strategy > controller > first available
        if self._apac_strategy and len(available_ids) > 1:
            target_id = self._apac_strategy.route_read(available_ids, self._metrics, bucket_id, key)
            ordered = [target_id] + [bid for bid in available_ids if bid != target_id]
        elif self._controller and len(available_ids) > 1:
            target_id = self._controller.route_read(available_ids, bucket_id, key)
            ordered = [target_id] + [bid for bid in available_ids if bid != target_id]
        else:
            ordered = available_ids

        req_id = f"{bucket_id}:{key}:{time.time()}"
        for bid in ordered:
            backend = self._backends.get(bid)
            if not backend:
                continue

            self._bus.emit(StorageEvent(
                event_type=EventType.QUEUE_ENQUEUED,
                backend_id=bid, bucket_id=bucket_id, key=key,
                meta={"request_id": req_id},
            ))
            self._bus.emit(StorageEvent(
                event_type=EventType.QUEUE_DEQUEUED,
                backend_id=bid, bucket_id=bucket_id, key=key,
                meta={"request_id": req_id},
            ))
            self._bus.emit(StorageEvent(EventType.GET_STARTED, bid, bucket_id, key))

            start = time.time()
            result = await backend.get(bucket_id, key)
            latency_ms = (time.time() - start) * 1000

            self._bus.emit(StorageEvent(
                event_type=EventType.QUEUE_COMPLETED,
                backend_id=bid, bucket_id=bucket_id, key=key,
                meta={"request_id": req_id},
            ))

            if result.is_ok:
                data = result.unwrap()
                self._bus.emit(StorageEvent(
                    event_type=EventType.GET_COMPLETED,
                    backend_id=bid, bucket_id=bucket_id, key=key,
                    size_bytes=len(data), latency_ms=latency_ms, success=True,
                ))

                if self._apac_strategy:
                    asyncio.create_task(self._handle_scale(bucket_id, key, data))
                elif self._controller:
                    existing_ids = list(self._replica_map.get(self._rkey(bucket_id, key), []))
                    source_bid = bid

                    async def _fetch(_bid=source_bid, _bk=bucket_id, _k=key) -> bytes:
                        r = await self._backends[_bid].get(_bk, _k)
                        return r.unwrap() if r.is_ok else b""

                    asyncio.create_task(self._controller.check_and_replicate(
                        bucket_id=bucket_id,
                        key=key,
                        existing_backend_ids=existing_ids,
                        all_backends=list(self._backends.values()),
                        fetch_value=_fetch,
                        tags={},
                        content_type="application/octet-stream",
                    ))
                return Ok(data)

            self._bus.emit(StorageEvent(
                event_type=EventType.GET_FAILED,
                backend_id=bid, bucket_id=bucket_id, key=key,
                latency_ms=latency_ms, success=False,
            ))

        return Err(Exception(f"All backends failed for get({bucket_id!r}, {key!r})"))

    # ---- list ----

    async def list(self, bucket_id: str) -> Result[List[str], Exception]:
        for backend in self._backends.values():
            result = await backend.list(bucket_id)
            if result.is_ok:
                self._bus.emit(StorageEvent(EventType.LIST_COMPLETED, backend.backend_id, bucket_id, "*"))
                return result
        return Err(Exception(f"All backends failed for list({bucket_id!r})"))

    # ---- delete ----

    async def delete(self, bucket_id: str, key: str) -> Result[bool, Exception]:
        holders = self._backends_holding(bucket_id, key) or list(self._backends.values())
        results = await asyncio.gather(*[b.delete(bucket_id, key) for b in holders], return_exceptions=True)
        any_ok = any(
            not isinstance(r, Exception) and hasattr(r, "is_ok") and r.is_ok
            for r in results
        )
        if any_ok:
            self._replica_map.pop(self._rkey(bucket_id, key), None)
            self._bus.emit(StorageEvent(EventType.DELETE_COMPLETED, "*", bucket_id, key))
            return Ok(True)
        return Err(Exception(f"All backends failed for delete({bucket_id!r}, {key!r})"))

    # ---- elastic scaling ----

    async def _handle_scale(self, bucket_id: str, key: str, data: bytes) -> None:
        """
        Called after each successful GET when an APaCStrategy is attached.
        Asks the strategy whether to scale up or down, then acts on the decision.
        """
        if not self._apac_strategy:
            return
        rkey = self._rkey(bucket_id, key)
        existing_ids = list(self._replica_map.get(rkey, []))
        decision = await self._apac_strategy.evaluate_scale(
            bucket_id=bucket_id,
            key=key,
            metrics=self._metrics,
            all_backends=list(self._backends.values()),
            existing_backend_ids=existing_ids,
            bus=self._bus,
        )
        if decision is None:
            return

        if decision.action == "up":
            for new_backend in decision.backends_to_add:
                self.add_backend(new_backend)
                # Replicate data to the new backend
                rep_result = await self._apac_strategy.replication_strategy.replicate(
                    backends=[new_backend],
                    bucket_id=bucket_id,
                    key=key,
                    value=data,
                    tags={},
                    content_type="application/octet-stream",
                    bus=self._bus,
                )
                if rep_result.is_ok:
                    self._replica_map.setdefault(rkey, []).append(new_backend.backend_id)
                    self._bus.emit(StorageEvent(
                        event_type=EventType.SCALE_UP,
                        backend_id=new_backend.backend_id,
                        bucket_id=bucket_id,
                        key=key,
                        meta={"mode": self._apac_strategy.mode.value},
                    ))

        elif decision.action == "down":
            for bid in decision.backend_ids_to_remove:
                self.remove_backend(bid)
                rkey_list = self._replica_map.get(rkey, [])
                if bid in rkey_list:
                    rkey_list.remove(bid)
                self._bus.emit(StorageEvent(
                    event_type=EventType.SCALE_DOWN,
                    backend_id=bid,
                    bucket_id=bucket_id,
                    key=key,
                    meta={"mode": self._apac_strategy.mode.value},
                ))


# ── Concrete storage systems ───────────────────────────────────────────────────

class ActiveStorageSystem(_ReplicatedStorageSystem):
    """All-or-nothing replication; no auto-scaling."""

    def __init__(
        self,
        backends: List[AbstractStorageBackend],
        controller: Optional[APaCController] = None,
        bus: Optional[EventBus] = None,
    ) -> None:
        super().__init__(
            backends=backends,
            replication_strategy=ActiveReplicationStrategy(),
            controller=controller,
            bus=bus,
        )


class PassiveStorageSystem(_ReplicatedStorageSystem):
    """First-wins replication; no auto-scaling."""

    def __init__(
        self,
        backends: List[AbstractStorageBackend],
        controller: Optional[APaCController] = None,
        bus: Optional[EventBus] = None,
    ) -> None:
        super().__init__(
            backends=backends,
            replication_strategy=PassiveReplicationStrategy(),
            controller=controller,
            bus=bus,
        )


class ElasticActiveStorageSystem(_ReplicatedStorageSystem):
    """All-or-nothing replication with elastic backend scaling."""

    def __init__(
        self,
        backends: List[AbstractStorageBackend],
        strategy: Optional[Any] = None,   # ElasticActiveStrategy
        bus: Optional[EventBus] = None,
    ) -> None:
        from mictlanx.apac.strategies import ElasticActiveStrategy
        super().__init__(
            backends=backends,
            replication_strategy=ActiveReplicationStrategy(),
            apac_strategy=strategy or ElasticActiveStrategy(),
            bus=bus,
        )


class ElasticPassiveStorageSystem(_ReplicatedStorageSystem):
    """First-wins replication with elastic backend scaling."""

    def __init__(
        self,
        backends: List[AbstractStorageBackend],
        strategy: Optional[Any] = None,   # ElasticPassiveStrategy
        bus: Optional[EventBus] = None,
    ) -> None:
        from mictlanx.apac.strategies import ElasticPassiveStrategy
        super().__init__(
            backends=backends,
            replication_strategy=PassiveReplicationStrategy(),
            apac_strategy=strategy or ElasticPassiveStrategy(),
            bus=bus,
        )


class HEAPaCStorageSystem(_ReplicatedStorageSystem):
    """
    MictlanX-native elastic storage system.  Uses HEAPaCStrategy to monitor
    Storage Pools via AsyncRouter and provision/decommission peers via Summoner.
    """

    def __init__(
        self,
        backends: List[AbstractStorageBackend],
        strategy: Any,                     # HEAPaCStrategy (required)
        bus: Optional[EventBus] = None,
    ) -> None:
        super().__init__(
            backends=backends,
            replication_strategy=PassiveReplicationStrategy(),
            apac_strategy=strategy,
            bus=bus,
        )

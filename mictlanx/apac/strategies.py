from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Literal, Optional
import random
import time

from mictlanx.apac.events import EventBus, EventType, StorageEvent
from mictlanx.apac.metrics import MetricsCollector
from mictlanx.apac.replication import (
    AbstractReplicationStrategy,
    ActiveReplicationStrategy,
    PassiveReplicationStrategy,
    ReplicaOutcome,
    ReplicationResult,
)


class APaCMode(Enum):
    NONE = "none"
    ACTIVE = "active"
    PASSIVE = "passive"
    ELASTIC_ACTIVE = "elastic_active"
    ELASTIC_PASSIVE = "elastic_passive"
    HE_APAC = "he_apac"


@dataclass
class ScaleDecision:
    """
    Returned by AbstractAPaCStrategy.evaluate_scale() to tell the storage system
    what to do next.

    - action="up"   → add backends_to_add to the pool and replicate to them
    - action="down" → evict backend_ids_to_remove from the pool
    """
    action: Literal["up", "down"]
    backends_to_add: List[Any] = field(default_factory=list)        # AbstractStorageBackend
    backend_ids_to_remove: List[str] = field(default_factory=list)


class _SingleBackendReplicationStrategy(AbstractReplicationStrategy):
    """Writes only to the FIRST backend in the provided list (NoneStrategy)."""

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
        if not backends:
            return ReplicationResult(bucket_id=bucket_id, key=key, total=0, succeeded=0)
        target = backends[0]
        bus.emit(StorageEvent(EventType.PUT_STARTED, target.backend_id, bucket_id, key))
        start = time.time()
        result = await target.put(bucket_id, key, value, tags, content_type)
        latency_ms = (time.time() - start) * 1000
        if result.is_ok:
            bus.emit(StorageEvent(
                event_type=EventType.PUT_COMPLETED,
                backend_id=target.backend_id,
                bucket_id=bucket_id,
                key=key,
                size_bytes=len(value),
                latency_ms=latency_ms,
                success=True,
            ))
            bus.emit(StorageEvent(
                event_type=EventType.REPLICA_AVAILABLE,
                backend_id=target.backend_id,
                bucket_id=bucket_id,
                key=key,
            ))
            return ReplicationResult(
                bucket_id=bucket_id,
                key=key,
                total=1,
                succeeded=1,
                replicas=[ReplicaOutcome(backend_id=target.backend_id, success=True, latency_ms=latency_ms)],
                first_available_backend_id=target.backend_id,
            )
        err = result.unwrap_err()
        bus.emit(StorageEvent(
            event_type=EventType.PUT_FAILED,
            backend_id=target.backend_id,
            bucket_id=bucket_id,
            key=key,
            latency_ms=latency_ms,
            success=False,
            meta={"error": str(err)},
        ))
        return ReplicationResult(
            bucket_id=bucket_id, key=key, total=1, succeeded=0,
            replicas=[ReplicaOutcome(backend_id=target.backend_id, success=False, latency_ms=latency_ms, error=err)],
        )


class AbstractAPaCStrategy(ABC):
    """
    Defines how a storage system replicates data, routes reads, and scales its
    backend pool.  The six concrete strategies form a spectrum from no-ops
    (NoneStrategy) to full elastic peer management (HEAPaCStrategy).
    """

    @property
    @abstractmethod
    def mode(self) -> APaCMode: ...

    @property
    @abstractmethod
    def replication_strategy(self) -> AbstractReplicationStrategy: ...

    @abstractmethod
    def route_read(
        self,
        available_backend_ids: List[str],
        metrics: MetricsCollector,
        bucket_id: str,
        key: str,
    ) -> str:
        """Return the backend_id to route the next GET to."""
        ...

    @abstractmethod
    async def evaluate_scale(
        self,
        bucket_id: str,
        key: str,
        metrics: MetricsCollector,
        all_backends: List[Any],          # List[AbstractStorageBackend]
        existing_backend_ids: List[str],  # backends that already hold this key
        bus: EventBus,
    ) -> Optional[ScaleDecision]:
        """
        Inspect current metrics and return a ScaleDecision if the pool should
        grow or shrink, or None if no action is needed.
        """
        ...


# ── None ──────────────────────────────────────────────────────────────────────

class NoneStrategy(AbstractAPaCStrategy):
    """
    No replication.  Writes go to exactly one backend (the first in the pool).
    Never scales.  Reads are always served from the first available backend.
    """

    @property
    def mode(self) -> APaCMode:
        return APaCMode.NONE

    @property
    def replication_strategy(self) -> AbstractReplicationStrategy:
        return _SingleBackendReplicationStrategy()

    def route_read(self, available_backend_ids, metrics, bucket_id, key) -> str:
        return available_backend_ids[0]

    async def evaluate_scale(self, bucket_id, key, metrics, all_backends, existing_backend_ids, bus) -> None:
        return None


# ── Active / Passive ──────────────────────────────────────────────────────────

class ActiveStrategy(AbstractAPaCStrategy):
    """
    All-or-nothing replication across all configured backends.  No auto-scaling.
    Reads are routed to the least-loaded backend.
    """

    @property
    def mode(self) -> APaCMode:
        return APaCMode.ACTIVE

    @property
    def replication_strategy(self) -> AbstractReplicationStrategy:
        return ActiveReplicationStrategy()

    def route_read(self, available_backend_ids, metrics, bucket_id, key) -> str:
        if len(available_backend_ids) == 1:
            return available_backend_ids[0]
        return metrics.least_loaded_backend_id(available_backend_ids)

    async def evaluate_scale(self, bucket_id, key, metrics, all_backends, existing_backend_ids, bus) -> None:
        return None


class PassiveStrategy(AbstractAPaCStrategy):
    """
    First-wins replication — the object is available as soon as one backend
    succeeds.  No auto-scaling.  Reads are routed to the least-loaded backend.
    """

    @property
    def mode(self) -> APaCMode:
        return APaCMode.PASSIVE

    @property
    def replication_strategy(self) -> AbstractReplicationStrategy:
        return PassiveReplicationStrategy()

    def route_read(self, available_backend_ids, metrics, bucket_id, key) -> str:
        if len(available_backend_ids) == 1:
            return available_backend_ids[0]
        return metrics.least_loaded_backend_id(available_backend_ids)

    async def evaluate_scale(self, bucket_id, key, metrics, all_backends, existing_backend_ids, bus) -> None:
        return None


# ── Elastic Active / Elastic Passive ──────────────────────────────────────────

class ElasticActiveStrategy(AbstractAPaCStrategy):
    """
    Active replication with elastic scaling.

    - Scale UP  when total reads for a key >= scale_up_reads AND replicas < max_replicas.
    - Scale DOWN when total reads for a key <= scale_down_reads AND replicas > min_replicas.

    Active semantics are preserved: REPLICA_AVAILABLE only fires once ALL backends succeed.
    """

    def __init__(
        self,
        scale_up_reads: int = 10,
        scale_down_reads: int = 2,
        min_replicas: int = 1,
        max_replicas: int = 5,
    ) -> None:
        self.scale_up_reads = scale_up_reads
        self.scale_down_reads = scale_down_reads
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas

    @property
    def mode(self) -> APaCMode:
        return APaCMode.ELASTIC_ACTIVE

    @property
    def replication_strategy(self) -> AbstractReplicationStrategy:
        return ActiveReplicationStrategy()

    def route_read(self, available_backend_ids, metrics, bucket_id, key) -> str:
        if len(available_backend_ids) == 1:
            return available_backend_ids[0]
        return metrics.least_loaded_backend_id(available_backend_ids)

    async def evaluate_scale(
        self,
        bucket_id: str,
        key: str,
        metrics: MetricsCollector,
        all_backends: List[Any],
        existing_backend_ids: List[str],
        bus: EventBus,
    ) -> Optional[ScaleDecision]:
        total_reads = metrics.get_counter(bucket_id, key)
        current = len(existing_backend_ids)
        existing_set = set(existing_backend_ids)

        if total_reads >= self.scale_up_reads and current < self.max_replicas:
            candidates = [b for b in all_backends if b.backend_id not in existing_set]
            if candidates:
                return ScaleDecision(action="up", backends_to_add=[candidates[0]])

        elif total_reads <= self.scale_down_reads and current > self.min_replicas:
            if existing_backend_ids:
                least_read = metrics.least_read_backend_id(existing_backend_ids, bucket_id, key)
                return ScaleDecision(action="down", backend_ids_to_remove=[least_read])

        return None


class ElasticPassiveStrategy(AbstractAPaCStrategy):
    """
    Passive replication with elastic scaling.

    Same scaling thresholds as ElasticActiveStrategy, but passive availability
    semantics: REPLICA_AVAILABLE fires as soon as the FIRST write succeeds.
    """

    def __init__(
        self,
        scale_up_reads: int = 10,
        scale_down_reads: int = 2,
        min_replicas: int = 1,
        max_replicas: int = 5,
    ) -> None:
        self.scale_up_reads = scale_up_reads
        self.scale_down_reads = scale_down_reads
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas

    @property
    def mode(self) -> APaCMode:
        return APaCMode.ELASTIC_PASSIVE

    @property
    def replication_strategy(self) -> AbstractReplicationStrategy:
        return PassiveReplicationStrategy()

    def route_read(self, available_backend_ids, metrics, bucket_id, key) -> str:
        if len(available_backend_ids) == 1:
            return available_backend_ids[0]
        return metrics.least_loaded_backend_id(available_backend_ids)

    async def evaluate_scale(
        self,
        bucket_id: str,
        key: str,
        metrics: MetricsCollector,
        all_backends: List[Any],
        existing_backend_ids: List[str],
        bus: EventBus,
    ) -> Optional[ScaleDecision]:
        total_reads = metrics.get_counter(bucket_id, key)
        current = len(existing_backend_ids)
        existing_set = set(existing_backend_ids)

        if total_reads >= self.scale_up_reads and current < self.max_replicas:
            candidates = [b for b in all_backends if b.backend_id not in existing_set]
            if candidates:
                return ScaleDecision(action="up", backends_to_add=[candidates[0]])

        elif total_reads <= self.scale_down_reads and current > self.min_replicas:
            if existing_backend_ids:
                least_read = metrics.least_read_backend_id(existing_backend_ids, bucket_id, key)
                return ScaleDecision(action="down", backend_ids_to_remove=[least_read])

        return None


# ── HE-APaC (MictlanX) ────────────────────────────────────────────────────────

class HEAPaCStrategy(AbstractAPaCStrategy):
    """
    Highly-Elastic APaC strategy — the only strategy that manages MictlanX
    storage peers directly.

    Scale-up path:
      1. Scan all configured StoragePools for health and available disk.
      2. Pick the pool with the most available disk capacity.
      3. Call Summoner.summon_peer() to provision a new peer in that pool.
      4. Wrap it as a MictlanXBackend and return ScaleDecision("up", [new_backend]).

    Scale-down path:
      1. Find the least-read replica across existing backends.
      2. Call Summoner.delete_container() to decommission it.
      3. Return ScaleDecision("down", [backend_id]).

    Routing: delegates to least-loaded backend (queue depth + in-flight).
    Replication: passive semantics (object available immediately on first peer success).
    """

    def __init__(
        self,
        pools: List[Any],           # List[StoragePool]
        summoner: Any,              # Summoner
        min_peers: int = 1,
        max_peers: int = 5,
        scale_up_reads: int = 10,   # trigger scale-up when total reads >= this
        scale_up_disk_uf: float = 0.8,  # also scale-up when pool disk utilization > 80%
    ) -> None:
        self.pools = pools
        self.summoner = summoner
        self.min_peers = min_peers
        self.max_peers = max_peers
        self.scale_up_reads = scale_up_reads
        self.scale_up_disk_uf = scale_up_disk_uf
        self._used_ports: List[int] = []
        self._peer_counter: int = 0

    @property
    def mode(self) -> APaCMode:
        return APaCMode.HE_APAC

    @property
    def replication_strategy(self) -> AbstractReplicationStrategy:
        return PassiveReplicationStrategy()

    def route_read(self, available_backend_ids, metrics, bucket_id, key) -> str:
        if len(available_backend_ids) == 1:
            return available_backend_ids[0]
        return metrics.least_loaded_backend_id(available_backend_ids)

    async def evaluate_scale(
        self,
        bucket_id: str,
        key: str,
        metrics: MetricsCollector,
        all_backends: List[Any],
        existing_backend_ids: List[str],
        bus: EventBus,
    ) -> Optional[ScaleDecision]:
        total_reads = metrics.get_counter(bucket_id, key)
        total_peers = len(all_backends)

        # ── scale down ────────────────────────────────────────────────────────
        if total_reads == 0 and total_peers > self.min_peers and len(existing_backend_ids) > self.min_peers:
            least_read = metrics.least_read_backend_id(existing_backend_ids, bucket_id, key)
            result = self.summoner.delete_container(container_id=least_read)
            if result.is_ok:
                return ScaleDecision(action="down", backend_ids_to_remove=[least_read])
            return None

        # ── scale up ──────────────────────────────────────────────────────────
        if total_peers >= self.max_peers:
            return None

        if total_reads < self.scale_up_reads:
            return None

        # pick the healthiest pool with the most available disk
        best_pool = None
        best_disk = -1
        for pool in self.pools:
            if not await pool.is_healthy():
                continue
            disk = await pool.available_disk_bytes()
            if disk > best_disk:
                best_disk = disk
                best_pool = pool

        if best_pool is None:
            return None

        # provision a new peer in the selected pool
        port = self._next_port()
        self._peer_counter += 1
        container_id = f"mictlanx-peer-apac-{self._peer_counter}"

        result = self.summoner.summon_peer(
            container_id=container_id,
            port=port,
            selected_node=best_pool.pool_id,
        )
        if result.is_err:
            return None

        summon_resp = result.unwrap()
        best_pool.peer_ids.append(container_id)

        # build a MictlanXBackend from the summoned peer
        from mictlanx.services import AsyncPeer
        from mictlanx.apac.backends.mictlanx_backend import MictlanXBackend
        new_peer = AsyncPeer(
            peer_id=container_id,
            ip_addr=summon_resp.ip_addr,
            port=summon_resp.port,
            protocol="http",
            api_version=best_pool.router.api_version,
        )
        return ScaleDecision(action="up", backends_to_add=[MictlanXBackend(peer=new_peer)])

    def _next_port(self) -> int:
        port = random.randint(30000, 60000)
        while port in self._used_ports:
            port = random.randint(30000, 60000)
        self._used_ports.append(port)
        return port

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List
import time

from mictlanx.apac.events import EventBus, EventType, StorageEvent


@dataclass
class ReplicaMetrics:
    """Per-replica counters and latency accumulators for a single (backend, bucket, key) triple."""
    backend_id: str
    bucket_id: str
    key: str
    read_count: int = 0
    write_count: int = 0
    read_bytes: int = 0
    write_bytes: int = 0
    last_read_at: float = 0.0
    last_write_at: float = 0.0
    total_read_latency_ms: float = 0.0
    total_write_latency_ms: float = 0.0
    _first_seen: float = field(default=0.0, repr=False)

    def popularity_score(self, decay_window_secs: float = 3600.0) -> float:
        """Read count weighted by recency — decays to zero after decay_window_secs with no access."""
        if self.last_read_at == 0.0:
            return 0.0
        age = time.time() - self.last_read_at
        decay = max(0.0, 1.0 - age / decay_window_secs)
        return self.read_count * decay

    def access_frequency(self) -> float:
        """Reads per second since this replica was first written."""
        if self._first_seen == 0.0 or self.read_count == 0:
            return 0.0
        elapsed = max(1.0, time.time() - self._first_seen)
        return self.read_count / elapsed

    def avg_read_latency_ms(self) -> float:
        return self.total_read_latency_ms / self.read_count if self.read_count else 0.0

    def avg_write_latency_ms(self) -> float:
        return self.total_write_latency_ms / self.write_count if self.write_count else 0.0


@dataclass
class QueueMetrics:
    """
    Tracks request queue state for a single backend.

    queue_depth  = enqueued - dequeued   (waiting to start)
    in_flight    = dequeued  - completed  (being processed)
    """
    backend_id: str
    _enqueued: int = field(default=0, repr=False)
    _dequeued: int = field(default=0, repr=False)
    _completed: int = field(default=0, repr=False)
    _total_wait_ms: float = field(default=0.0, repr=False)
    _enqueue_times: Dict[str, float] = field(default_factory=dict, repr=False)

    @property
    def queue_depth(self) -> int:
        return max(0, self._enqueued - self._dequeued)

    @property
    def in_flight(self) -> int:
        return max(0, self._dequeued - self._completed)

    @property
    def total_load(self) -> int:
        """Waiting + in-flight — the total number of requests the backend is handling."""
        return self.queue_depth + self.in_flight

    @property
    def avg_wait_ms(self) -> float:
        return self._total_wait_ms / self._completed if self._completed else 0.0

    def enqueue(self, request_id: str) -> None:
        self._enqueued += 1
        self._enqueue_times[request_id] = time.time()

    def dequeue(self, request_id: str) -> None:
        self._dequeued += 1

    def complete(self, request_id: str) -> None:
        self._completed += 1
        enqueue_t = self._enqueue_times.pop(request_id, time.time())
        self._total_wait_ms += (time.time() - enqueue_t) * 1000


class MetricsCollector:
    """
    Subscribes to the EventBus and maintains:
    - per-(backend, bucket, key) ReplicaMetrics  →  used for popularity and read routing
    - per-backend QueueMetrics                    →  used for load-aware routing and queue analysis

    Exposes two scalar helpers that map directly to the AvailabilityPolicy `when` variables:
        get_counter(bucket_id, key)           →  $GET_COUNTER
        access_frequency_pct(bucket_id, key)  →  $ACCESS_FREQUENCY (0-100 %)
    """

    def __init__(self, bus: EventBus) -> None:
        self._replicas: Dict[str, ReplicaMetrics] = {}
        self._queues: Dict[str, QueueMetrics] = {}
        bus.subscribe_all(self._on_event)

    # ---- internal helpers ----

    def _rkey(self, backend_id: str, bucket_id: str, key: str) -> str:
        return f"{backend_id}\x00{bucket_id}\x00{key}"

    def _replica(self, backend_id: str, bucket_id: str, key: str) -> ReplicaMetrics:
        k = self._rkey(backend_id, bucket_id, key)
        if k not in self._replicas:
            self._replicas[k] = ReplicaMetrics(backend_id=backend_id, bucket_id=bucket_id, key=key)
        return self._replicas[k]

    def _queue(self, backend_id: str) -> QueueMetrics:
        if backend_id not in self._queues:
            self._queues[backend_id] = QueueMetrics(backend_id=backend_id)
        return self._queues[backend_id]

    def _on_event(self, ev: StorageEvent) -> None:
        rm = self._replica(ev.backend_id, ev.bucket_id, ev.key)
        qm = self._queue(ev.backend_id)
        t = ev.event_type

        if t == EventType.GET_COMPLETED and ev.success:
            rm.read_count += 1
            rm.read_bytes += ev.size_bytes
            rm.last_read_at = ev.timestamp
            rm.total_read_latency_ms += ev.latency_ms
        elif t == EventType.PUT_COMPLETED and ev.success:
            rm.write_count += 1
            rm.write_bytes += ev.size_bytes
            rm.last_write_at = ev.timestamp
            rm.total_write_latency_ms += ev.latency_ms
            if rm._first_seen == 0.0:
                rm._first_seen = ev.timestamp
        elif t == EventType.QUEUE_ENQUEUED:
            qm.enqueue(ev.meta.get("request_id", ""))
        elif t == EventType.QUEUE_DEQUEUED:
            qm.dequeue(ev.meta.get("request_id", ""))
        elif t == EventType.QUEUE_COMPLETED:
            qm.complete(ev.meta.get("request_id", ""))

    # ---- public query API ----

    def replica(self, backend_id: str, bucket_id: str, key: str) -> ReplicaMetrics:
        return self._replica(backend_id, bucket_id, key)

    def queue(self, backend_id: str) -> QueueMetrics:
        return self._queue(backend_id)

    def replicas_for_key(self, bucket_id: str, key: str) -> List[ReplicaMetrics]:
        return [r for r in self._replicas.values() if r.bucket_id == bucket_id and r.key == key]

    def queues(self) -> Dict[str, QueueMetrics]:
        return dict(self._queues)

    # ---- AvailabilityPolicy `when` variable evaluators ----

    def get_counter(self, bucket_id: str, key: str) -> int:
        """Total reads across all replicas — maps to $GET_COUNTER."""
        return sum(r.read_count for r in self.replicas_for_key(bucket_id, key))

    def access_frequency_pct(self, bucket_id: str, key: str) -> float:
        """Aggregate access frequency as a 0-100 normalised value — maps to $ACCESS_FREQUENCY."""
        rms = self.replicas_for_key(bucket_id, key)
        if not rms:
            return 0.0
        return min(100.0, sum(r.access_frequency() for r in rms) * 100)

    def top_popular(self, n: int = 10) -> List[ReplicaMetrics]:
        return sorted(self._replicas.values(), key=lambda r: r.popularity_score(), reverse=True)[:n]

    def least_loaded_backend_id(self, backend_ids: List[str]) -> str:
        """Return the backend_id with the lowest total load among the given candidates."""
        if not backend_ids:
            raise ValueError("backend_ids must not be empty")
        return min(backend_ids, key=lambda bid: self._queue(bid).total_load)

    def least_read_backend_id(self, backend_ids: List[str], bucket_id: str, key: str) -> str:
        """Return the backend_id with the fewest reads for this key — spreads read load."""
        if not backend_ids:
            raise ValueError("backend_ids must not be empty")
        return min(backend_ids, key=lambda bid: self._replica(bid, bucket_id, key).read_count)

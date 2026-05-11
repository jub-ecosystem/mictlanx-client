from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List
import time


class EventType(Enum):
    PUT_STARTED = auto()
    PUT_COMPLETED = auto()
    PUT_FAILED = auto()
    GET_STARTED = auto()
    GET_COMPLETED = auto()
    GET_FAILED = auto()
    LIST_COMPLETED = auto()
    DELETE_COMPLETED = auto()
    REPLICA_AVAILABLE = auto()     # passive: emitted on first successful write
    REPLICATION_COMPLETE = auto()  # all replicas confirmed (active: all; passive: all done)
    REPLICA_CREATED = auto()       # APaC spawned a new replica in response to load
    QUEUE_ENQUEUED = auto()
    QUEUE_DEQUEUED = auto()
    QUEUE_COMPLETED = auto()
    SCALE_UP = auto()              # elastic: a new backend/peer was added to the pool
    SCALE_DOWN = auto()            # elastic: a backend/peer was removed from the pool


@dataclass
class StorageEvent:
    event_type: EventType
    backend_id: str
    bucket_id: str
    key: str
    timestamp: float = field(default_factory=time.time)
    size_bytes: int = 0
    latency_ms: float = 0.0
    success: bool = True
    meta: Dict[str, Any] = field(default_factory=dict)


EventHandler = Callable[[StorageEvent], None]


class EventBus:
    def __init__(self) -> None:
        self._by_type: Dict[EventType, List[EventHandler]] = {}
        self._global: List[EventHandler] = []

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        self._by_type.setdefault(event_type, []).append(handler)

    def subscribe_all(self, handler: EventHandler) -> None:
        self._global.append(handler)

    def emit(self, event: StorageEvent) -> None:
        for h in self._by_type.get(event.event_type, []):
            h(event)
        for h in self._global:
            h(event)

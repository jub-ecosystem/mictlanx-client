from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple

from mictlanx.apac.contextual_lang import AvailabilityPolicy, Inequality
from mictlanx.apac.metrics import MetricsCollector
from mictlanx.apac.events import EventBus, EventType, StorageEvent
from mictlanx.apac.replication import AbstractReplicationStrategy, ActiveReplicationStrategy


_OPS = {
    ">":  lambda a, b: a > b,
    "<":  lambda a, b: a < b,
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    "!=": lambda a, b: a != b,
}

# Maps an AvailabilityPolicy `when` variable name to a MetricsCollector method.
# Each callable receives (metrics, bucket_id, key) -> float.
_VAR_RESOLVERS: Dict[str, Any] = {
    "GET_COUNTER":       lambda m, bid, key: float(m.get_counter(bid, key)),
    "ACCESS_FREQUENCY":  lambda m, bid, key: m.access_frequency_pct(bid, key),
    "ACCESS_FREQUENNCY": lambda m, bid, key: m.access_frequency_pct(bid, key),  # typo in existing DSL
}


def _evaluate_inequality(inq: Inequality, metrics: MetricsCollector, bucket_id: str, key: str) -> bool:
    """Return True if the Inequality condition holds for the given (bucket_id, key)."""
    resolver = _VAR_RESOLVERS.get(inq.variable.upper())
    if resolver is None:
        return False
    actual = resolver(metrics, bucket_id, key)
    # value may be "10", "60.6", "60.6%" — strip trailing %
    raw = str(inq.value).rstrip("%")
    try:
        threshold = float(raw)
    except ValueError:
        return False
    op = _OPS.get(inq.symbol)
    return op(actual, threshold) if op else False


class APaCController:
    """
    Evaluates AvailabilityPolicy `when` conditions against live MetricsCollector
    data and reacts in two ways:

    1. **Routing**: `route_read(bucket_ids, bucket_id, key)` returns the backend_id
       to which a GET should be routed to minimise read latency (least reads + load).

    2. **Auto-replication**: `check_and_replicate(bucket_id, key, value, tags, backends)`
       evaluates the `when` predicates; if any is satisfied it spawns a background
       task that copies the object to backends that do not yet hold a replica.
    """

    def __init__(
        self,
        policy: AvailabilityPolicy,
        metrics: MetricsCollector,
        bus: EventBus,
        strategy: Optional[AbstractReplicationStrategy] = None,
    ) -> None:
        self._policy = policy
        self._metrics = metrics
        self._bus = bus
        self._strategy = strategy or ActiveReplicationStrategy()

    # ---- routing ----

    def route_read(self, available_backend_ids: List[str], bucket_id: str, key: str) -> str:
        """
        Return the backend_id to route the next read to.

        Selection order:
          1. Least total_load (queue_depth + in_flight) among available backends.
          2. Tie-break: fewest reads for this specific (bucket_id, key).
        """
        if not available_backend_ids:
            raise ValueError("available_backend_ids must not be empty")
        if len(available_backend_ids) == 1:
            return available_backend_ids[0]
        return self._metrics.least_loaded_backend_id(available_backend_ids)

    # ---- condition evaluation ----

    def conditions_met(self, bucket_id: str, key: str) -> Tuple[bool, List[str]]:
        """
        Evaluate every `when` condition in the AvailabilityPolicy against
        current metrics for (bucket_id, key).

        Returns (any_met: bool, reasons: list[str]).
        """
        reasons: List[str] = []
        for identifier, inq in self._policy.when.items():
            if _evaluate_inequality(inq, self._metrics, bucket_id, key):
                reasons.append(f"{identifier}: {inq}")
        return bool(reasons), reasons

    # ---- auto-replication ----

    async def check_and_replicate(
        self,
        bucket_id: str,
        key: str,
        existing_backend_ids: List[str],
        all_backends: List[Any],  # List[AbstractStorageBackend]
        fetch_value: Any,         # async callable () -> bytes
        tags: Dict[str, str] = {},
        content_type: str = "application/octet-stream",
    ) -> None:
        """
        If AvailabilityPolicy `when` conditions are satisfied, copy the object
        to any `where` backends that do not yet hold a replica.

        `fetch_value` is an async zero-argument callable that returns the raw bytes
        (called lazily — only if replication is actually needed).
        """
        any_met, reasons = self.conditions_met(bucket_id, key)
        if not any_met:
            return

        backends_by_id = {b.backend_id: b for b in all_backends}
        existing = set(existing_backend_ids)

        # `where` in the policy lists peer/node identifiers.  We match them against
        # backend_ids by substring so "pool-0.peer-1" can refer to "mictlanx-peer-1".
        target_backends = [
            b for bid, b in backends_by_id.items()
            if bid not in existing and any(w in bid or bid in w for w in self._policy.where)
        ]
        if not target_backends:
            # Fall back: any backend that doesn't yet hold a replica.
            target_backends = [b for bid, b in backends_by_id.items() if bid not in existing]
        if not target_backends:
            return

        value = await fetch_value()
        result = await self._strategy.replicate(
            backends=target_backends,
            bucket_id=bucket_id,
            key=key,
            value=value,
            tags=tags,
            content_type=content_type,
            bus=self._bus,
        )
        for outcome in result.replicas:
            if outcome.success:
                self._bus.emit(StorageEvent(
                    event_type=EventType.REPLICA_CREATED,
                    backend_id=outcome.backend_id,
                    bucket_id=bucket_id,
                    key=key,
                    meta={"reasons": reasons, "how": self._policy.how},
                ))

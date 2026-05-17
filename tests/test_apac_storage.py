"""
Unit tests for the APaC storage layer.

These tests run entirely in-process with an in-memory stub backend —
no live peers, routers, S3, or cloud accounts required.
"""
from __future__ import annotations
from typing import Dict, List
import pytest

from option import Ok, Err
from mictlanx.apac.backends.base import AbstractStorageBackend
from mictlanx.apac.events import EventBus, EventType, StorageEvent
from mictlanx.apac.metrics import MetricsCollector
from mictlanx.apac.replication import ActiveReplicationStrategy, PassiveReplicationStrategy
from mictlanx.apac.storage import ActiveStorageSystem, PassiveStorageSystem
from mictlanx.apac.controller import APaCController, _evaluate_inequality
from mictlanx.apac.contextual_lang import AvailabilityPolicy, Inequality


# ---- in-memory stub backend ----

class MemoryBackend(AbstractStorageBackend):
    def __init__(self, bid: str, fail: bool = False) -> None:
        self._id = bid
        self._store: Dict[str, bytes] = {}
        self._fail = fail

    @property
    def backend_id(self) -> str:
        return self._id

    async def put(self, bucket_id, key, value, tags={}, content_type="application/octet-stream"):
        if self._fail:
            return Err(Exception("simulated failure"))
        self._store[f"{bucket_id}::{key}"] = value
        return Ok({"key": key})

    async def get(self, bucket_id, key):
        v = self._store.get(f"{bucket_id}::{key}")
        if v is None:
            return Err(KeyError(f"{bucket_id}/{key} not found"))
        return Ok(v)

    async def list(self, bucket_id):
        prefix = f"{bucket_id}::"
        return Ok([k.removeprefix(prefix) for k in self._store if k.startswith(prefix)])

    async def delete(self, bucket_id, key):
        self._store.pop(f"{bucket_id}::{key}", None)
        return Ok(True)

    async def health(self):
        return True


# ---- helpers ----

def make_policy(
    when_var: str = "GET_COUNTER",
    symbol: str = ">",
    threshold: str = "5",
    how: str = "ACTIVE",
    where: List[str] = [],
) -> AvailabilityPolicy:
    return AvailabilityPolicy(
        available_resources={},
        who="",
        what=[],
        where=where,
        how=how,
        when={"bucket0": Inequality(variable=when_var, symbol=symbol, value=threshold)},
        version="v1",
    )


# ---- replication strategy tests ----

@pytest.mark.asyncio
async def test_active_replication_all_succeed():
    bus = EventBus()
    events: List[StorageEvent] = []
    bus.subscribe_all(events.append)

    b0, b1 = MemoryBackend("b0"), MemoryBackend("b1")
    strategy = ActiveReplicationStrategy()
    result = await strategy.replicate([b0, b1], "bk", "k1", b"hello", {}, "text/plain", bus)

    assert result.all_ok
    assert result.succeeded == 2
    assert result.first_available_backend_id == "b0"
    assert b"hello" == b0._store["bk::k1"]
    assert b"hello" == b1._store["bk::k1"]

    types = [e.event_type for e in events]
    assert EventType.REPLICATION_COMPLETE in types
    assert EventType.REPLICA_AVAILABLE in types


@pytest.mark.asyncio
async def test_active_replication_one_fails_no_available_event():
    bus = EventBus()
    events: List[StorageEvent] = []
    bus.subscribe_all(events.append)

    b0, b_fail = MemoryBackend("b0"), MemoryBackend("b_fail", fail=True)
    strategy = ActiveReplicationStrategy()
    result = await strategy.replicate([b0, b_fail], "bk", "k1", b"data", {}, "text/plain", bus)

    assert not result.all_ok
    assert result.succeeded == 1
    assert result.first_available_backend_id is None  # active: NOT available when any fails

    types = [e.event_type for e in events]
    assert EventType.REPLICATION_COMPLETE not in types
    assert EventType.REPLICA_AVAILABLE not in types


@pytest.mark.asyncio
async def test_passive_replication_available_on_first_success():
    bus = EventBus()
    events: List[StorageEvent] = []
    bus.subscribe_all(events.append)

    b0, b1 = MemoryBackend("b0"), MemoryBackend("b1")
    strategy = PassiveReplicationStrategy()
    result = await strategy.replicate([b0, b1], "bk", "k1", b"hello", {}, "text/plain", bus)

    assert result.is_ok
    assert result.first_available_backend_id in ("b0", "b1")

    # REPLICA_AVAILABLE must be emitted (even if one backend fails later)
    assert any(e.event_type == EventType.REPLICA_AVAILABLE for e in events)


@pytest.mark.asyncio
async def test_passive_replication_still_available_when_one_fails():
    bus = EventBus()
    b_ok, b_fail = MemoryBackend("b_ok"), MemoryBackend("b_fail", fail=True)
    strategy = PassiveReplicationStrategy()
    result = await strategy.replicate([b_ok, b_fail], "bk", "k1", b"data", {}, "text/plain", bus)

    assert result.is_ok
    assert result.succeeded == 1
    assert result.first_available_backend_id == "b_ok"


# ---- storage system tests ----

@pytest.mark.asyncio
async def test_active_storage_put_and_get():
    b0, b1 = MemoryBackend("b0"), MemoryBackend("b1")
    sys = ActiveStorageSystem(backends=[b0, b1])

    put_res = await sys.put("bk", "k1", b"world")
    assert put_res.is_ok

    get_res = await sys.get("bk", "k1")
    assert get_res.is_ok
    assert get_res.unwrap() == b"world"


@pytest.mark.asyncio
async def test_passive_storage_put_and_get():
    b0, b1 = MemoryBackend("b0"), MemoryBackend("b1")
    sys = PassiveStorageSystem(backends=[b0, b1])

    put_res = await sys.put("bk", "k1", b"passive_data")
    assert put_res.is_ok

    get_res = await sys.get("bk", "k1")
    assert get_res.is_ok
    assert get_res.unwrap() == b"passive_data"


@pytest.mark.asyncio
async def test_storage_list_and_delete():
    b0 = MemoryBackend("b0")
    sys = ActiveStorageSystem(backends=[b0])
    await sys.put("bk", "k1", b"v1")
    await sys.put("bk", "k2", b"v2")

    list_res = await sys.list("bk")
    assert list_res.is_ok
    assert set(list_res.unwrap()) == {"k1", "k2"}

    del_res = await sys.delete("bk", "k1")
    assert del_res.is_ok

    list_res2 = await sys.list("bk")
    assert "k1" not in list_res2.unwrap()


@pytest.mark.asyncio
async def test_storage_get_falls_back_on_failure():
    b_fail = MemoryBackend("b_fail", fail=True)
    b_ok = MemoryBackend("b_ok")
    b_ok._store["bk::k1"] = b"fallback"
    sys = ActiveStorageSystem(backends=[b_fail, b_ok])
    # Manually populate replica map so both backends are tried
    sys._replica_map["bk\x00k1"] = ["b_fail", "b_ok"]

    res = await sys.get("bk", "k1")
    assert res.is_ok
    assert res.unwrap() == b"fallback"


# ---- metrics tests ----

def test_metrics_get_counter():
    bus = EventBus()
    mc = MetricsCollector(bus)
    for _ in range(7):
        bus.emit(StorageEvent(
            event_type=EventType.GET_COMPLETED,
            backend_id="b0",
            bucket_id="bk",
            key="k1",
            size_bytes=10,
            success=True,
        ))
    assert mc.get_counter("bk", "k1") == 7


def test_metrics_queue_tracking():
    bus = EventBus()
    mc = MetricsCollector(bus)
    bus.emit(StorageEvent(EventType.QUEUE_ENQUEUED, "b0", "bk", "k1", meta={"request_id": "r1"}))
    bus.emit(StorageEvent(EventType.QUEUE_ENQUEUED, "b0", "bk", "k1", meta={"request_id": "r2"}))
    assert mc.queue("b0").queue_depth == 2
    bus.emit(StorageEvent(EventType.QUEUE_DEQUEUED, "b0", "bk", "k1", meta={"request_id": "r1"}))
    assert mc.queue("b0").queue_depth == 1
    assert mc.queue("b0").in_flight == 1


def test_metrics_least_loaded_backend():
    bus = EventBus()
    mc = MetricsCollector(bus)
    # b0 has 3 enqueued; b1 has 1
    for rid in ["r1", "r2", "r3"]:
        bus.emit(StorageEvent(EventType.QUEUE_ENQUEUED, "b0", "bk", "k1", meta={"request_id": rid}))
    bus.emit(StorageEvent(EventType.QUEUE_ENQUEUED, "b1", "bk", "k1", meta={"request_id": "rx"}))

    assert mc.least_loaded_backend_id(["b0", "b1"]) == "b1"


# ---- controller / condition evaluation tests ----

def test_evaluate_inequality_get_counter():
    bus = EventBus()
    mc = MetricsCollector(bus)
    for _ in range(11):
        bus.emit(StorageEvent(EventType.GET_COMPLETED, "b0", "bk", "k1", size_bytes=1, success=True))

    inq = Inequality(variable="GET_COUNTER", symbol=">", value="10")
    assert _evaluate_inequality(inq, mc, "bk", "k1") is True

    inq_false = Inequality(variable="GET_COUNTER", symbol=">", value="100")
    assert _evaluate_inequality(inq_false, mc, "bk", "k1") is False


@pytest.mark.asyncio
async def test_controller_conditions_met_triggers_replication():
    bus = EventBus()
    mc = MetricsCollector(bus)
    policy = make_policy(when_var="GET_COUNTER", symbol=">", threshold="5", where=["b1"])

    b0, b1 = MemoryBackend("b0"), MemoryBackend("b1")
    b0._store["bk::k1"] = b"original"

    controller = APaCController(policy=policy, metrics=mc, bus=bus)

    # Simulate 6 reads — should cross the $GET_COUNTER > 5 threshold
    for _ in range(6):
        bus.emit(StorageEvent(EventType.GET_COMPLETED, "b0", "bk", "k1", size_bytes=8, success=True))

    any_met, reasons = controller.conditions_met("bk", "k1")
    assert any_met, f"expected conditions to be met; reasons={reasons}"

    events: List[StorageEvent] = []
    bus.subscribe_all(events.append)

    async def fetch() -> bytes:
        return b0._store["bk::k1"]

    await controller.check_and_replicate(
        bucket_id="bk",
        key="k1",
        existing_backend_ids=["b0"],
        all_backends=[b0, b1],
        fetch_value=fetch,
        tags={},
    )

    assert b1._store.get("bk::k1") == b"original"
    assert any(e.event_type == EventType.REPLICA_CREATED for e in events)

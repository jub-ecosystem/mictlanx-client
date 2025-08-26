import pytest
from mictlanx.services import AsyncPeer
import os
from uuid import uuid4
import hashlib as H
@pytest.fixture
def peer() -> AsyncPeer:
    # Adjust the IP/port/protocol to point to your running service
    return AsyncPeer(peer_id="mictlanx-peer-x", ip_addr="localhost", port=26000, protocol="http")




@pytest.mark.asyncio
async def test_flush_tasks(peer):
    res = await peer.flush_tasks()
    assert res.is_ok

@pytest.mark.asyncio
async def test_get_all_ball_sizes(peer):
    res = await peer.get_all_ball_sizes(start=0,end=100)
    assert res.is_ok


@pytest.mark.asyncio
async def test_put_metadata_then_put_data_and_verify_roundtrip(peer:AsyncPeer):

    # If the peer isn’t up, skip the test with a helpful message
    flush = await peer.flush_tasks()
    if flush.is_err:
        pytest.skip(f"Peer not reachable: {flush.unwrap_err()}")

    bucket = os.environ.get("MICTLANX_TEST_BUCKET", "pytest-bucket")
    key    = f"pytest-{uuid4().hex}"
    data   = b"hello mictlanx!" * 1024  # ~16KB
    size   = len(data)
    sha256 = H.sha256(data).hexdigest()
    ctype  = "application/octet-stream"

    # 1) Put metadata -> obtain task_id
    meta = await peer.put_metadata(
        key=key,
        size=size,
        checksum=sha256,
        producer_id="pytest",
        content_type=ctype,
        ball_id=key,             # you can pick your own convention
        bucket_id=bucket,
        tags={"suite": "pytest"},
        timeout=30,
    )
    assert meta.is_ok, f"put_metadata failed: {meta.unwrap_err()}"
    task_id = meta.unwrap().task_id
    assert task_id and isinstance(task_id, str)

    # 2) Upload the data using the task_id
    put = await peer.put_data(task_id=task_id, key=key, value=data, content_type=ctype, timeout=60)
    assert put.is_ok, f"put_data failed: {put.unwrap_err()}"

    # 3) Verify size
    sz = await peer.get_size(bucket_id=bucket, key=key, timeout=30)
    assert sz.is_ok, f"get_size failed: {sz.unwrap_err()}"
    assert sz.unwrap().size == size

    # 4) (Optional) Download and check contents
    resp = await peer.get_streaming(bucket_id=bucket, key=key, timeout=30, verify=False)
    assert resp.is_ok, f"get_streaming failed: {resp.unwrap_err()}"
    assert resp.unwrap().content == data


# def test_get_balls_len(peer):
#     res = peer.get_balls_len()
#     assert res.is_ok

# def test_get_state(peer):
#     res = peer.get_state()
#     assert res.is_ok

# def test_get_stats(peer):
#     res = peer.get_stats()
#     assert res.is_ok

# def test_get_balls(peer):
#     res = peer.get_balls()
#     assert res.is_ok

# def test_add_peer(peer):
#     res = peer.add_peer(
#         id="peer-x",
#         disk=100,
#         memory=100,
#         ip_addr="127.0.0.1",
#         port=26000,
#         weight=1.0,
#     )
#     assert res.is_ok

# def test_add_peer_with_retry(peer):
#     res = peer.add_peer_with_retry(
#         id="peer-y",
#         disk=100,
#         memory=100,
#         ip_addr="127.0.0.1",
#         port=26001,
#         weight=1.0,
#     )
#     assert res.is_ok

# def test_replicate(peer):
#     res = peer.replicate(bucket_id="mictlanx", key="some-key")
#     assert res.is_ok

# def test_get_size(peer):
#     res = peer.get_size(bucket_id="mictlanx", key="some-key")
#     assert res.is_ok

# def test_delete(peer):
#     res = peer.delete(bucket_id="mictlanx", key="some-key")
#     assert res.is_ok

# def test_disable(peer):
#     res = peer.disable(bucket_id="mictlanx", key="some-key")
#     assert res.is_ok

# def test_get_metadata(peer):
#     res = peer.get_metadata(bucket_id="mictlanx", key="some-key")
#     assert res.is_ok

# def test_get_streaming(peer):
#     res = peer.get_streaming(bucket_id="mictlanx", key="some-key")
#     assert res.is_ok

# def test_get_bucket_metadata(peer):
#     res = peer.get_bucket_metadata(bucket_id="mictlanx")
#     assert res.is_ok

# def test_get_ufs(peer):
#     res = peer.get_ufs()
#     assert res.is_ok

# def test_get_ufs_with_retry(peer):
#     res = peer.get_ufs_with_retry()
#     assert res.is_ok

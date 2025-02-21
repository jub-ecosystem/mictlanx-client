from mictlanx.interfaces import AsyncPeer
import pytest
import asyncio


@pytest.mark.asyncio  # ✅ Required for async test functions
async def test_get_stats():
    p   = AsyncPeer(peer_id="mictlanx-peer-0", ip_addr="localhost",port=24000, protocol="http")
    res = await  p.get_stats()
    print("RES",res)
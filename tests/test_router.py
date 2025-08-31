import pytest
from mictlanx.services import AsyncRouter
from xolo.utils.utils import Utils as XoloUtils
from mictlanx.utils.index import Utils
@pytest.fixture
def router() -> AsyncRouter:
    # Adjust the IP/port/protocol to point to your running service
    return AsyncRouter(
        router_id   = "mictlanx-peer-x",
        ip_addr     = "localhost",
        port        = 60666,
        protocol    = "http",
        http2       = False,
        api_version = 4
    )


@pytest.mark.asyncio
async def test_get_stats(router:AsyncRouter):
    res = await router.get_stats()
    print(res)
@pytest.mark.asyncio
async def test_put(router:AsyncRouter):
    data     = b"Hi from pytest"
    checksum = XoloUtils.sha256(data)
    res = await router.put_metadata(
        bucket_id          = "bxxxxx",
        key                = "k1",
        ball_id            = "b1",
        checksum           = checksum,
        content_type       = "text/plain",
        headers            = {"Task-Id":"t1"},
        is_disabled        = False,
        producer_id        = "mictlanx",
        replication_factor = 2,
        size               = len(data),
        tags               = {"test":"1"},
        timeout            = 120,
        verify             = False

    )
    assert res.is_ok
    ts = res.unwrap()
    # print(ts)
    for task_id in ts.tasks_ids:
        chunks = Utils.to_async_gen_bytes(data=data,chunk_size="256kb")
        ress = await router.put_chunked(
            task_id = task_id,
            chunks  = chunks
        )
        print(ress)
        assert ress.is_ok
    # ts.tas
    # res = router.
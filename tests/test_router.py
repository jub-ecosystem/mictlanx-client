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
    assert res.is_ok
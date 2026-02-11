import os
import pytest
from mictlanx.services import Summoner
from mictlanx.services.models.summoner import SummonContainerPayload,ExposedPort,MountX,MountType
from option import Some,NONE

import dotenv

dotenv.load_dotenv(".env.test")

MICTLANX_SUMMONER_PORT = int(os.environ.get("MICTLANX_SUMMONER_PORT", "15000"))

@pytest.fixture()
def summoner():
    return Summoner(
        ip_addr     = "localhost",
        api_version = Some(3),
        network     = NONE,
        port        = MICTLANX_SUMMONER_PORT,
        protocol    = "http"
    )

def test_health_check(summoner:Summoner):
    res = summoner.health_check()
    assert res.is_ok

def test_stats(summoner:Summoner):
    res = summoner.stats()
    print(res)
    assert res.is_ok

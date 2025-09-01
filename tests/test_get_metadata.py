
import pytest
import os
import httpx
import asyncio
from mictlanx import AsyncClient
import time as T
from mictlanx.utils.compression import CompressionAlgorithm
# from mictlanx.interfaces import Metadata
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.uri import MictlanXURI


uri     = os.environ.get("MICTLANX_URI","mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0")
client = AsyncClient(
    client_id    = os.environ.get("CLIENT_ID","client-0"),
    uri=uri,
    debug        = True,
    max_workers  = 8,
    log_output_path= os.environ.get("MICTLANX_LOG_PATH","/mictlanx/client")
)

@pytest.fixture
def bucket_id_param(request:pytest.FixtureRequest):
    return request.config.getoption("--bucketid", default="b1")

@pytest.fixture
def key_param(request:pytest.FixtureRequest):
    return request.config.getoption("--key",default="x")

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_get_metadata_by_ball_id(bucket_id_param,key_param):
    x = await client.get_metadata(bucket_id=bucket_id_param, ball_id=key_param)
    if x.is_err:
        assert "Failed to get metadata"
    print(x)
    m = x.unwrap()
    print(m)
@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_get_metadata_by_key(bucket_id_param,key_param):
    x = await client.get_metadata_by_key(bucket_id=bucket_id_param, key=key_param)
    if x.is_err:
        assert "Failed to get metadata"
    m = x.unwrap()
    print(m)

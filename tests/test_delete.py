import os
from mictlanx.asyncx import AsyncClient
import pytest
import dotenv 
dotenv.load_dotenv()

uri = os.environ.get("MICTLANX_URI","mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0")

client = AsyncClient(
    client_id    = os.environ.get("CLIENT_ID","client-0"),
    uri= uri,
    debug        = True,
    max_workers  = 2,
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
async def test_delete(bucket_id_param,key_param):
    # bucket_id       = "bucket-0"
    x = await client.delete(
        bucket_id=bucket_id_param,
        ball_id=key_param
    )
    print(x)
    assert x.is_ok
@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_delete_bucket(bucket_id_param):
    # bucket_id       = "bucket-0"
    x = await client.delete_bucket(
        bucket_id=bucket_id_param,
        force=True
    )
    assert x.is_ok




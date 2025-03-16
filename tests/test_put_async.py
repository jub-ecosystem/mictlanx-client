import os
import sys
from mictlanx.v4.asyncx import AsyncClient
import pytest
import asyncio
from option import Some
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils
from mictlanx.utils.segmentation import Chunks
from mictlanx.utils.compression import CompressionAlgorithm
import humanfriendly as HF


DEFAULT_BUCKET_ID = "b1"
routers     = Utils.routers_from_str(
    routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
    protocol=os.environ.get("MICTLANX_PROTOCOL","http")
) 

client = AsyncClient(
    client_id    = os.environ.get("CLIENT_ID","client-0"),
    routers        = list(routers),
    debug        = True,
    max_workers  = 2,
    log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
)
    

@pytest.fixture
def bucket_id_param(request:pytest.FixtureRequest):
    return request.config.getoption("--bucketid",default="b1")
@pytest.fixture
def key_param(request:pytest.FixtureRequest):
    return request.config.getoption("--key",default="x")

# @pytest.mark.skip("")
@pytest.mark.asyncio  # ✅ Required for async test functions
async def test_put_chunks(bucket_id_param,key_param):
    # key       = "mypdf"
    # path      = "/source/01.pdf"
    key       = str(key_param)
    path      = "/source/f50mb"
    rf        = 1
    bucket_id = str(bucket_id_param)
    chunk_size = "25MB"
    chunks_maybe = Chunks.from_file(path=path, group_id= key, chunk_size = Some(HF.parse_size(chunk_size)))
    if chunks_maybe.is_none:
        assert False
    chunks = chunks_maybe.unwrap()
    x = await client.put_chunks(
        bucket_id  = bucket_id,
        chunk_size = chunk_size,
        key        = key,
        rf         = rf,
        chunks      = chunks,
        max_tries  = 10
    )
    print(x)
    assert x.is_ok
@pytest.mark.skip("")
@pytest.mark.asyncio  # ✅ Required for async test functions
async def test_put_file(bucket_id_param,key_param):
    # key       = "mypdf"
    # path      = "/source/01.pdf"
    key       = str(key_param)
    path      = "/source/f50mb"
    rf        = 1
    bucket_id = str(bucket_id_param)
    chunk_size = "25MB"

    x = await client.put_file(
        bucket_id  = bucket_id,
        chunk_size = chunk_size,
        key        = key,
        rf         = rf,
        path      = path,
        max_tries  = 10
    )
    print(x)
    assert x.is_ok
        # print(x)
@pytest.mark.skip("")
@pytest.mark.asyncio  # ✅ Required for async test functions
async def test_put(bucket_id_param,key_param):
    # key       = "mypdf"
    # path      = "/source/01.pdf"
    key       = str(key_param)
    path      = "/source/f50mb"
    rf        = 1
    bucket_id = str(bucket_id_param)
    chunk_size = "25MB"

    with open(path,"rb") as f:
        data = f.read()
        x = await client.put(
            bucket_id  = bucket_id,
            chunk_size = chunk_size,
            key        = key,
            rf         = rf,
            value      = data,
            max_tries  = 10
        )
        print(x)
        assert x.is_ok
        # print(x)

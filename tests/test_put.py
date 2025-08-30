import os
import sys
from mictlanx import AsyncClient
import pytest
import asyncio
from option import Some
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.uri import MictlanXURI
from mictlanx.utils.segmentation import Chunks,Chunk
from mictlanx.utils.compression import CompressionAlgorithm
import humanfriendly as HF


DEFAULT_BUCKET_ID = "b1"
uri = os.environ.get("MICTLANX_URI","mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0")


client = AsyncClient(
    client_id       = os.environ.get("CLIENT_ID","client-0"),
    uri             = uri,
    debug           = True,
    max_workers     = 2,
    log_output_path = os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
)
    

@pytest.fixture
def bucket_id_param(request:pytest.FixtureRequest):
    return request.config.getoption("--bucketid",default="b1")
@pytest.fixture
def key_param(request:pytest.FixtureRequest):
    return request.config.getoption("--key",default="x")

@pytest.mark.skip("")
@pytest.mark.asyncio  # ✅ Required for async test functions
async def test_put_chunks(bucket_id_param,key_param):
    # key       = "mypdf"
    # path      = "/source/01.pdf"
    bucket_id = str(bucket_id_param)

    key       = str(key_param)

    # path      = "/source/01.pdf"
    path      = "/source/burrito.gif"
    
    rf        = 1
    
    chunk_size = "256kb"
    
    chunks_maybe = Chunks.from_file(
        path=path,
        group_id= key,
        chunk_size = Some(HF.parse_size(chunk_size))
    )
    
    if chunks_maybe.is_none:
        assert False

    chunks = chunks_maybe.unwrap()

    x = await client.put_chunks(
        bucket_id  = bucket_id,
        key        = key,
        rf         = rf,
        chunks      = chunks,
        max_tries  = 1
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
        path       = path,
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
            max_tries  = 1,
            tags={
                "test":"kjhshjfhjsfsf",
                "value":"9823984892342"
            }
        )
        print(x)
        assert x.is_ok
        # print(x)


@pytest.mark.skip("")
@pytest.mark.asyncio  # ✅ Required for async test functions
async def test_put_single_chunk(bucket_id_param,key_param):
    # key       = "mypdf"
    # path      = "/source/01.pdf"
    key       = str(key_param)
    rf        = 1
    bucket_id = str(bucket_id_param)
    chunk_size = "25MB"


    index =1
    chunk = Chunk(group_id=key, index=index, data=b"HOLAAAAAAA", chunk_id=Some(f"{key}_{index}"),metadata={"a":"aa"})
    print("CHUNK_ID",chunk.chunk_id)
    x = await client.put_single_chunk(
        bucket_id  = bucket_id,
        chunk_size = chunk_size,
        ball_id    = key,
        rf         = rf,
        chunk      = chunk,
        max_tries  = 1,
        tags={
            "test":"kjhshjfhjsfsf",
            "value":"9823984892342"
        }
    )
    print(x)
    assert x.is_ok
    # print(x)

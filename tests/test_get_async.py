
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
from mictlanx.utils.index import Utils
from mictlanx.utils.uri import MictlanXURI

uri = os.environ.get("MICTLANX_URI","mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0")

client = AsyncClient(
    client_id       = os.environ.get("CLIENT_ID","client-0"),
    uri             = uri,
    debug           = True,
    max_workers     = 8,
    log_output_path = os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
)

@pytest.fixture
def bucket_id_param(request:pytest.FixtureRequest):
    return request.config.getoption("--bucketid", default="b1")

@pytest.fixture
def key_param(request:pytest.FixtureRequest):
    return request.config.getoption("--key",default="x")


@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_get_chunk(bucket_id_param,key_param):
    res = await client.get_chunk(
        bucket_id=bucket_id_param,
        ball_id=key_param,
        index=1,
        backoff_factor=4,
        max_retries=5
    )
    print(res)
    assert res.is_ok

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_simple_raw_get(bucket_id_param,key_param):
    url = "https://148.247.201.141:60667/api/v4/buckets/b0/x_2"

    total_start = T.perf_counter()
    headers = {
    "User-Agent": "curl/7.87.0",       # or whatever your cURL says in logs
    "Accept": "*/*",                  # cURL default
    "Host": "148.247.201.141:60667",  # if cURL is sending that
    "Accept-Encoding": "identity",         # or none, to match cURL exactly
    "Chunk-Size":"100000",

    }
    conn_start = T.perf_counter()
    async with httpx.AsyncClient(verify=False,http2=True, http1=False, timeout=120,headers=headers) as client:
        conn_done = T.perf_counter()

        request_start = T.perf_counter()
        response = await client.get(url)
        request_done = T.perf_counter()
        print("HEADERS", response.headers)
        # Force-read the entire body
        body_start = T.perf_counter()
        _ = response.content
        body_done = T.perf_counter()

    total_end = T.perf_counter()

    print(f"Connection setup:    {conn_done - conn_start:.4f}s")
    print(f"Request/1st byte:    {request_done - request_start:.4f}s")
    print(f"Read full response:  {body_done - body_start:.4f}s")
    print(f"Total time:          {total_end - total_start:.4f}s")


@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_get_to_file(bucket_id_param,key_param):
    bucket_id         = str(bucket_id_param)
    key               = str(key_param)
    start_time        = T.time()
    MAX_GETS          = 1
    chunk_size        = "1mb"
    max_parallel_reqs = 1
    for i in range(MAX_GETS):
        
        x_result = await client.get_to_file(
            bucket_id=bucket_id,
            ball_id=key,
            output_path="/source/test",
            # fullname="test_xx.pdf",
            max_paralell_gets=max_parallel_reqs,
            chunk_size=chunk_size,
            force=True
        )
        if x_result.is_ok:
            xx = x_result.unwrap()
            print("PATH", xx)
        # print("x",x)
        # .get_with_retry(bucket_id=bucket_id, key=key, force=force,chunk_size=chunk_size)
        assert x_result.is_ok
    print(f"TOTAL_RESPONSE_TIME: {T.time()-start_time}")
    # print(x)

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_get(bucket_id_param,key_param):
    bucket_id         = str(bucket_id_param)
    key               = str(key_param)
    start_time        = T.time()
    MAX_GETS          = 1
    chunk_size        = "1mb"
    max_parallel_reqs = 1
    for i in range(MAX_GETS):
        
        x_result = await client.get(
            bucket_id=bucket_id,
            key=key,
            max_paralell_gets=max_parallel_reqs,
            chunk_size=chunk_size,
            force=True,
            chunk_index=0
            
        )
        if x_result.is_ok:
            xx = x_result.unwrap()
        # print("x",x)
        # .get_with_retry(bucket_id=bucket_id, key=key, force=force,chunk_size=chunk_size)
        assert x_result.is_ok
    print(f"TOTAL_RESPONSE_TIME: {T.time()-start_time}")
    # print(x)

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_get_chunks(bucket_id_param,key_param):
    bucket_id         = str(bucket_id_param)
    key               = str(key_param)

    start_time        = T.time()
    MAX_GETS          = 1
    chunk_size        = "1mb"
    max_parallel_reqs = 1

    for i in range(MAX_GETS):
        
        x_gen = client.get_chunks(
            bucket_id         = bucket_id,
            key               = key,
            max_parallel_gets = max_parallel_reqs,
            chunk_size        = chunk_size,
            backoff_factor    = 1.5,
            chunk_index=0
        )
        async for x in x_gen:
            print(x)
        # print("x",x)
        # .get_with_retry(bucket_id=bucket_id, key=key, force=force,chunk_size=chunk_size)
    print(f"TOTAL_RESPONSE_TIME: {T.time()-start_time}")
    # print(x)


import pytest
import os
from mictlanx import Client
import time as T
from mictlanx.utils.compression import CompressionAlgorithm
# from mictlanx.interfaces import Metadata
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils

peers     = Utils.routers_from_str(
routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
protocol=os.environ.get("MICTLANX_PROTOCOL","http")
) 

client = Client(
client_id    = os.environ.get("CLIENT_ID","client-0"),
routers        = list(peers),
debug        = True,
max_workers  = 8,
bucket_id= "",
log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
)

@pytest.mark.skip("")
def test_get():
    bucket_id  = "b1"
    key        = "f50mb"
    start_time = T.time()
    force      = False
    MAX_GETS   = 1
    # chunk_size = "256kb"
    chunk_size = "1mb"
    for i in range(MAX_GETS):
        
        x = client.get_with_retry(bucket_id=bucket_id, key=key, force=force,chunk_size=chunk_size)
        assert x.is_ok
    print(f"TOTAL_RESPONSE_TIME: {T.time()-start_time}")
    # print(x)

@pytest.mark.skip("")
def test_get_decompress():
    bucket_id  = "b1"
    key        = "f50mbcompressedlz41"
    start_time = T.time()
    force      = False
    MAX_GETS   = 1
    chunk_size = "10MB"
    compression_algorithm = CompressionAlgorithm.LZ4
    for i in range(MAX_GETS):
        
        x = client.get_and_decompress(bucket_id=bucket_id, key=key, force=force,chunk_size=chunk_size,compression_algorithm=compression_algorithm)
        assert x.is_ok
    print(f"TOTAL_RESPONSE_TIME: {T.time()-start_time}")
    # print(x)

# @pytest.mark.skip("")
def test_get_chunks():
    bucket_id  = "b1"
    key        = "fchunkx"
    start_time = T.time()
    force      = False
    MAX_GETS   = 1
    # chunk_size = "10MB"
    # compression_algorithm = CompressionAlgorithm.LZ4
    
    for i in range(MAX_GETS):
        x = client.get_and_merge_with_num_chunks(key=key, bucket_id=bucket_id, num_chunks=53)
        print(x)
        # x = client.get_and_decompress(bucket_id=bucket_id, key=key, force=force,chunk_size=chunk_size,compression_algorithm=compression_algorithm)
    #     assert x.is_ok
    # print(f"TOTAL_RESPONSE_TIME: {T.time()-start_time}")
    # # print(x)

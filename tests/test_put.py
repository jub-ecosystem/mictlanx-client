import os
from mictlanx import Client
import pytest
from option import Some
# from pytest import mark
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils
from mictlanx.utils.segmentation import Chunks
from mictlanx.utils.compression import CompressionAlgorithm
import humanfriendly as HF


DEFAULT_BUCKET_ID = "b1"
peers     = Utils.routers_from_str(
    routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
    protocol=os.environ.get("MICTLANX_PROTOCOL","http")
) 

client = Client(
    client_id    = os.environ.get("CLIENT_ID","client-0"),
    routers        = list(peers),
    debug        = True,
    max_workers  = 2,
    bucket_id= DEFAULT_BUCKET_ID,
    log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
)
    
@pytest.mark.skip("")
def test_put():
    key       = "f50mb"
    path      = "/source/f50mb"
    rf        = 1

    # client.delete
    x = client.put_file_chunked(
        path=path,
        chunk_size="10MB",
        bucket_id=DEFAULT_BUCKET_ID,
        key=key,
        tags={
            "example":"01_test_put.py"
        },
        # content_type="",
        replication_factor=rf
    )
    print(x)
    assert x.is_ok
    # print(x)


@pytest.mark.skip("")
def test_compress_and_put():
    key       = "f50mbcompressedlz41"
    path      = "/source/f50mb"
    rf        = 1

    # client.delete
    ca = CompressionAlgorithm.LZ4
    with open(path,"rb") as f:
        x = client.compress_and_put(
            chunk_size="10MB",
            bucket_id=DEFAULT_BUCKET_ID,
            key=key,
            value=f.read(),
            tags={
                "example":"01_test_put.py"
            },
            # content_type="application/pdf",
            replication_factor=rf,
            compression_algorithm=ca
        )
        print(x)
        assert x.is_ok


# @pytest.mark.skip("")
def test_put_chunks():
    key       = "fchunkx"
    path      = "/source/f50mb"
    rf        = 1
    chunk_size = HF.parse_size("1MB")
    # num_chunks = 10
    with open(path,"rb") as f:
        x = client.put_chunks_from_bytes(
            bucket_id=DEFAULT_BUCKET_ID,
            key=key,
            value=f.read(),
            tags={
                "example":"01_test_put.py"
            },
            # content_type="application/pdf",
            replication_factor=rf,
        )
        gen_list = list(x)
        completed = list(map(lambda x: x.is_ok , gen_list))
        assert all(completed)
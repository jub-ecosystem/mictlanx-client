import os
from mictlanx import Client
import pytest
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils


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


# @pytest.mark.skip("")
def test_delete_bucket():
    bucket_id       = "b1"
    x = client.delete_bucket_async( 
        bucket_id=bucket_id
    )
    assert x.is_ok


@pytest.mark.skip("")
def test_delete_by_bid():
    key       = "fchunkx"

    # client.delete
    x = client.delete_by_ball_id( 
        bucket_id="b1",
        ball_id = key
    )
    assert x.is_ok


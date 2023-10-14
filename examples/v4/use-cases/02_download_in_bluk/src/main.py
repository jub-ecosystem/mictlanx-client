
import os
import time as T
from scipy import stats as S
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import PutResponse
from mictlanx.utils.index import Utils
from mictlanx.logger.log import Log
from option import Result
from typing import Awaitable,List,Generator,Tuple
from concurrent.futures import as_completed
from option import Result
from mictlanx.v4.interfaces.responses import GetBucketMetadataResponse
# import json as J
# import pandas as pd
import dotenv 
env_file_path = os.environ.get("ENV_FILE_PATH","")
if not len(env_file_path) == 0:
    dotenv.load_dotenv(env_file_path)

# Basic logger from MictlanX utils
L = Log(
    name          = "test",
    create_folder = False,
    to_file       = False,
    error_log     = False
)
# Trace 
trace_path = os.environ.get("TRACE_PATH","")
# Space-separated string that contains basic info of the peers.
peers_str = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000")
# Parse the peers_str to a Peer object
peers = list(Utils.peers_from_str(peers_str))
# Create an instance of MictlanX - Client 
L.debug("TRACE_PATH={}".format(trace_path))
c = Client(
    # Unique identifier of the client
    client_id   = os.environ.get("MICTLANX_CLIENT_ID","client-0"),
    # Storage peers
    peers       = peers,
    # Number of threads to perform I/O operations
    max_workers = int(os.environ.get("MICTLANX_MAX_WORKERS","2")),
    # This parameters are optionals only set to True if you want to see some basic metrics ( this options increase little bit the overhead please take into account).
    debug       = True,
    # __________________________
    daemon      = False, 
)

res:Result[GetBucketMetadataResponse,Exception] = c.get_bucket_metadata(bucket_id= "MICTLANX_GLOBAL_BUCKET").result()
if res.is_ok:
    response:GetBucketMetadataResponse = res.unwrap()
    for ball in response.balls:
        result = c.get(key=ball.ball_id)
        print("GET",ball.ball_id)
    # print(response)
else:
    print("ERROR", res.unwrap_err())
# c.get
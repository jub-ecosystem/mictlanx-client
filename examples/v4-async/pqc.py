import os
import sys
from mictlanx.v4.asyncx import AsyncClient
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
    protocol=os.environ.get("MICTLANX_PROTOCOL","https")
) 

client = AsyncClient(
    client_id       = os.environ.get("CLIENT_ID","client-0"),

    routers         = list(routers),
    
    debug           = True,

    max_workers     = 2,
    
    log_output_path = os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
)


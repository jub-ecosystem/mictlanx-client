
import pytest
import os
import asyncio
from mictlanx.v4.asyncx import AsyncClient
import time as T
from typing import Dict,List,Iterator
import dotenv 
import humanfriendly as HF
dotenv.load_dotenv()
from mictlanx.utils.index import Utils
from typing import List, Dict, Any
from collections import defaultdict
import mictlanx.models as InterfaceX
import mictlanx.v4.models  as ModelX
peers     = Utils.routers_from_str(
    routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
    protocol=os.environ.get("MICTLANX_PROTOCOL","http")
) 

client = AsyncClient(
    client_id    = os.environ.get("CLIENT_ID","client-0"),
    routers        = list(peers),
    debug        = True,
    max_workers  = 8,
    log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
)


@pytest.mark.asyncio
async def test_get_balls_by_bucket_id():
    # balls:Dict[str,  List[str]] = {}

    bucket_id = "jcastillo"
    bucket_result = await client.get_bucket_metadata(bucket_id=bucket_id)
    assert bucket_result.is_ok
    bucket = bucket_result.unwrap()
    for b in bucket:
        print("full_path",b.full_path)
        print("brp",b.bucket_relative_path)
        print("fullname",b.fullname)
        print("filename",b.filename)
        print("extesion",b.extension)
        print("="*20)
    #     if not b.ball_id in balls:
    #         num_chunks = int(b.tags.get("num_chunks",0))
    #         balls[b.ball_id] = [f"{b.ball_id}_{i}" for i in range(num_chunks)]
    # print(balls)

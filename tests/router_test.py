

import os
import unittest as UT
import secrets
import time as T
import numpy as np
import requests as R
import humanfriendly as HF
from typing import Generator
from option import Some
from mictlanx.utils.index import Utils
from mictlanx.v4.client import Client
import pandas as pd
from dotenv import load_dotenv
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXParams
import logging
from mictlanx.v4.interfaces.index import Peer,Router
load_dotenv()
logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


class RouterXTest(UT.TestCase):
    BUCKET_ID = os.environ.get("BUCKET_ID","mictlanx")
    peers =  Utils.routers_from_str(
        routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
        protocol=os.environ.get("MICTLANX_PROTOCOL","http")
    ) 
    ylambda = 1
    # bucket_id = "public-bucket-0"

    
    client = Client(
        client_id    = os.environ.get("CLIENT_ID","client-0"),
        # 
        routers        = list(peers),
        # 
        debug        = True,
        # 
        max_workers  = 2,
        # 
        lb_algorithm ="2CHOICES_UF",
        bucket_id= BUCKET_ID,
        log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client"),
        tezcanalyticx_params=Some(
            TezcanalyticXParams(
                flush_timeout="30s",
                buffer_size=10,
                hostname="localhost",
                protocol="http",
                level=logging.INFO
            )
        )
    )

    def test_get_ufs_with_retry(self):
        router=  Router(
            "mictlanx-router-0",
            "http",
            "localhost",
            60666
        )
        

    @UT.skip("")
    def test_add_peers(self):
        router=  Router(
            "mictlanx-router-0",
            "http",
            "localhost",
            60666
        )
        res = router.add_peers(
            peers=[
                Peer(peer_id="pool-0.peer-0",ip_addr="pool-0.peer-0",port=56851,protocol="http"),
                Peer(peer_id="pool-0.peer-1",ip_addr="pool-0.peer-1",port=46045,protocol="http"),
                Peer(peer_id="pool-1.peer-0",ip_addr="pool-1.peer-0",port=48534,protocol="http")
            ]
        )
        print("RES",res)

    @UT.skip("")
    def test_elastic(self):
        router=  Router(
            "mictlanx-router-0",
            "http",
            "localhost",
            60666
        )
        res = router.elastic(
            rf=4
        )
        print("response",res)
    @UT.skip("")
    def test_replication(self):
        router=  Router(
            "mictlanx-router-0",
            "http",
            "localhost",
            60666
        )
        res = router.replication(
            rf=2,
            bucket_id="r8odbw5ihlxqoye3",
            key="myaxuq0wwbmbottr",
        )
        print("response",res)
        

if __name__ == "__main__":
    UT.main()

import os
import unittest as UT
import secrets
import time as T
import numpy as np
import requests as R
from typing import Generator
from option import Some
from mictlanx.utils.index import Utils
from mictlanx.v4.client import Client
import pandas as pd
from dotenv import load_dotenv
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXParams
import logging
from mictlanx.v4.interfaces.index import Peer

load_dotenv()


class MictlanXTest(UT.TestCase):
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
    @staticmethod
    def data_generator(num_chunks:int,n:int)->Generator[bytes,None,None]:
        for i in range(num_chunks):
            yield secrets.token_bytes(n)
    def test_replicate(self):
        ps =[
            Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=7000,protocol="http"),
            Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=7001,protocol="http"),
            Peer(peer_id="mictlanx-peer-2", ip_addr="localhost", port=7002,protocol="http"),
        ]
        for p in ps:
            try:
                response = p.replicate(bucket_id="activex",key="gxfamn3iru55coip")
                if response.is_err:
                    raise response.unwrap_err()
                else:
                    print("Response",response)
            except R.exceptions.RequestException as e:
                print(e.response.content)
            except Exception as e:
                print(e)

    @UT.skip("")
    def test_get_size(self):
        p = Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=7001,protocol="http")
        response = p.get_size(bucket_id="activex",key="ohhz6kflbykz4tju")
        print(response)
        return self.assertTrue(response.is_ok)

if __name__ == "__main__":
    UT.main()
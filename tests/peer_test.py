
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
from mictlanx import Client
from dotenv import load_dotenv
import mictlanx.v4.interfaces  as InterfaceX
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXParams
import logging
from mictlanx.v4.interfaces import Peer
load_dotenv()
logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


class MictlanXTest(UT.TestCase):
    BUCKET_ID = os.environ.get("BUCKET_ID","mictlanx")
    routers =  Utils.routers_from_str(
        routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
        protocol=os.environ.get("MICTLANX_PROTOCOL","http")
    ) 
    ylambda = 1
    # bucket_id = "public-bucket-0"

    
    client = Client(
        client_id    = os.environ.get("CLIENT_ID","client-0"),
        # 
        routers        = list(routers),
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
    

    def test_get_stats(self):
        peerss = [
            Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=25000,protocol="http"),
            Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=25001,protocol="http"),
            Peer(peer_id="mictlanx-peer-2", ip_addr="localhost", port=25003,protocol="http")
        ]
        stats = [ p.get_stats().unwrap_or(InterfaceX.PeerStatsResponse.empty()) for p in peerss]
        res = sum(stats, InterfaceX.PeerStatsResponse.empty())
        print("RES",res)
        # return self.assertTrue(res.is_ok)
    @UT.skip("")
    def test_flush_tasks(self):
        p1 = Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=25000,protocol="http")
        res = p1.flush_tasks()
        print("RES",res)

    @UT.skip("")
    def test_get_ufs_with_retry(self):
        p1 = Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=25000,protocol="http")
        res = p1.get_ufs_with_retry(logger=logger)
        print("RES",res)

    @UT.skip("")
    def test_get_all_balls_len(self):
        p1 = Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=25000,protocol="http")
        p2 = Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=25001,protocol="http")
        res = p1.get_all_ball_sizes()
        print("ALL_BALLS_LEN",res)

    @UT.skip("")
    def test_get_balls_len(self):
        p1 = Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=25000,protocol="http")
        p2 = Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=25001,protocol="http")
        res = p2.get_balls_len()
        print("BALLS_LEN",res)
    @UT.skip("")
    def test_get_state(self):
        p1 = Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=25000,protocol="http")
        p2 = Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=25001,protocol="http")
        res = p1.get_state(start=0,end=2)
        print(res)
    @UT.skip("")
    def test_get_balls(self):
        p1 = Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=25000,protocol="http")
        p2 = Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=25001,protocol="http")
        res = p2.get_balls(start=0, end=2)
        print("BALLS",res)

    @UT.skip("")
    def test_add_peer(self):
        p1 = Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=25000,protocol="http")
        p2 = Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=25001,protocol="http")
        res1 = p1.add_peer_with_retry(
            id="mictlanx-peer-1",
            disk=HF.parse_size("40GB"),
            memory=HF.parse_size("4GB"), 
            ip_addr="mictlanx-peer-1",
            port=25001, 
            weight=1,
            logger= logger
        )
        res2 = p2.add_peer_with_retry(
            id="mictlanx-peer-0",
            disk=HF.parse_size("40GB"),
            memory=HF.parse_size("4GB"),
            ip_addr="mictlanx-peer-0",
            port=25000,
            weight=1,
            logger= logger
        )
        logger.debug("res1 %s" ,res1)
        logger.debug("res2 %s", res2)
        return self.assertTrue(res1.is_ok and  res2.is_ok)


    @UT.skip("")
    def test_replicate(self):
        ps =[
            Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=7000,protocol="http"),
            Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=7001,protocol="http"),
            Peer(peer_id="mictlanx-peer-2", ip_addr="localhost", port=7002,protocol="http"),
        ]
        for p in ps:
            try:
                response = p.replicate(bucket_id="mictlanx",key="5c0ea09626a3796b99a8e030e6e302f8106ac2443add951c87550034e966408f")
                if response.is_err:
                    raise response.unwrap_err()
                else:
                    print("Response",response)
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
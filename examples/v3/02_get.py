import sys
import os
import time as T
from mictlanx.v3.client import Client
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.interfaces.payloads import AuthTokenPayload
from mictlanx.v3.services.replica_manger import ReplicaManager
from dotenv import load_dotenv
from option import Some

load_dotenv()
if __name__ =="__main__":
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v3/02_get.py <KEY>")
    key             = args[0]
    replica_manager = ReplicaManager(ip_addr = os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"), port=int(os.environ.get("MICTLANX_REPLICA_MANAGER_PORT",20000)), api_version=Some(3))
    xolo            = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port=int(os.environ.get("MICTLANX_XOLO_PORT",10000)), api_version=Some(3))
    proxy           = Proxy(ip_addr = os.environ.get("MICTLANX_PROXY_IP_ADDR"), port=int(os.environ.get("MICTLANX_PROXY_PORT",8080)), api_version=Some(3))
    c               = Client(
        app_id          = os.environ.get("MICTLANX_APP_ID"),
        client_id       = Some(os.environ.get("MICTLANX_CLIENT_ID")),
        replica_manager = replica_manager, 
        xolo            = xolo,
        proxies         = [proxy],
        secret          = os.environ.get("MICTLANX_SECRET"), 
        expires_in      = Some(os.environ.get("MICTLANX_EXPIRES_IN","1d") )
    )



    cache = True
    interarrival_time = 1
    MAX_DOWNLOADS = 1000

    for i in range(MAX_DOWNLOADS):
        res = c.get(key = key,cache= cache, force= i==0 )
        if(res.is_ok):
            response = res.unwrap()
            print("METADATA",response.metadata)
            print("RESPONSE_TIME",response.response_time)
            print("_"*20)
            T.sleep(interarrival_time)
        else:
            error = res.unwrap_err()
            print(error)
    c.logout()
import sys
import os
import numpy as np
import pandas as pd
from mictlanx.v3.interfaces.payloads import PutNDArrayPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.interfaces.payloads import AuthTokenPayload
from mictlanx.v3.services.replica_manger import ReplicaManager
# from nanoid import generate as nanoid_
from dotenv import load_dotenv
from option import Some

load_dotenv()
if __name__ =="__main__":
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v3/01_put.py <KEY> <PATH>")
    key  = args[0]
    path = args[1]
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
    ndarray         = pd.read_csv(path).values
    tags = {"tag1":"SOMETHING"}
    res = c.put_ndarray(key=key,ndarray=ndarray,tags=tags,update=True)
    for i in range(2):
        get_res = c.get_ndarray(key=key,cache=True,force=i==0)
        if(get_res.is_ok):
            get_response = get_res.unwrap()
            print("METADATA",get_response.metadata)
            print("VALUE",get_response.value)
            print("_"*20)
    c.logout()

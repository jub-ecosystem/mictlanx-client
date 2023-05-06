import sys
import os
from mictlanx.v3.interfaces.payloads import PutPayload
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
    replica_manager = ReplicaManager(ip_addr = os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"), port=int(os.environ.get("MICTLANX_REPLICA_MANAGER_PORT",20000)), api_version=3)
    xolo            = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port=int(os.environ.get("MICTLANX_XOLO_PORT",10000)), api_version=3)
    proxy           = Proxy(ip_addr = os.environ.get("MICTLANX_PROXY_IP_ADDR"), port=int(os.environ.get("MICTLANX_PROXY_PORT",8080)), api_version=3)
    c               = Client(
        app_id          = os.environ.get("MICTLANX_APP_ID"),
        client_id       = Some(os.environ.get("MICTLANX_CLIENT_ID")),
        replica_manager = replica_manager, 
        xolo            = xolo,
        proxy           = proxy,
        secret          = os.environ.get("MICTLANX_SECRET"), 
        expires_in      = Some(os.environ.get("MICTLANX_EXPIRES_IN","1d") )
    )
    res = c.get(key = key)
    if(res.is_ok):
        response = res.unwrap()
        print("METADATA",response.metadata)
    else:
        error = res.unwrap_err()
        print(error)
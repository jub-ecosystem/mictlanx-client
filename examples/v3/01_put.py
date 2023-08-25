import sys
import os
from mictlanx.v3.interfaces.payloads import PutPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.services.replica_manger import ReplicaManager
from dotenv import load_dotenv
from option import Some,NONE

load_dotenv()
if __name__ =="__main__":
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v3/01_put.py <KEY> <PATH>")
    key = args[0]
    path = args[1]

    #  1. Create a client.
    replica_manager  = ReplicaManager(ip_addr = os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"), port=int(os.environ.get("MICTLANX_REPLICA_MANAGER_PORT",20000)), api_version=Some(3))
    xolo             = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port=int(os.environ.get("MICTLANX_XOLO_PORT",10000)), api_version=Some(3))
    proxy            = Proxy(ip_addr = os.environ.get("MICTLANX_PROXY_IP_ADDR"), port=int(os.environ.get("MICTLANX_PROXY_PORT",8080)), api_version=Some(3))
    expires_in       = os.environ.get("MICTLANX_EXPIRES_IN","1d")
    # 1.1 The unique identifier of the application you belongs to.
    app_id           = os.environ.get("MICTLANX_APP_ID")
    # 1.2 Xolo Secret
    secret           = os.environ.get("MICTLANX_SECRET")
    c             = Client(
        app_id=app_id,
        client_id=Some(os.environ.get("MICTLANX_CLIENT_ID")),
        metadata=NONE,
        replica_manager=replica_manager,
        xolo = xolo,
        proxies=[proxy],
        secret=secret,
        expires_in=Some(expires_in)
    ) 
    # 2. Prepare the bytes to store in MictlanX
    with open(path,"rb") as f:
        data     = f.read()
        metadata = {}
        res      = c.put(
            key   = key,
            value = data,
            tags  = {
                "tag_1":"VALUE_1",
                "tag_2":"VALUE_2"
            },
            group_id="group_0",
        )
        print(res)
    c.logout()

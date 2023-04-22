import sys
import os
from mictlanx.v3.interfaces.payloads import PutPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.auth import Auth
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.interfaces.payloads import GenerateTokenPayload
from mictlanx.v3.services.replica_manger import ReplicaManager
from nanoid import generate as nanoid_
from dotenv import load_dotenv

load_dotenv()
if __name__ =="__main__":
    args = sys.argv[1:]
    # print(args)
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v3/01_put.py <KEY> <PATH>")
    key = args[0]
    path = args[1]
    rm_service    = ReplicaManager(ip_addr = os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"), port=int(os.environ.get("MICTLANX_REPLICA_MANAGER_PORT",20000)), api_version=3)
    auth_service  = Auth(ip_addr = os.environ.get("MICTLANX_AUTH_IP_ADDR"), port=int(os.environ.get("MICTLANX_AUTH_PORT",10000)), api_version=3)
    proxy_service = Proxy(ip_addr = os.environ.get("MICTLANX_PROXY_IP_ADDR"), port=int(os.environ.get("MICTLANX_PROXY_PORT",8080)), api_version=3)
    c             = Client(rm_service = rm_service, auth_service = auth_service,proxy = proxy_service,password = os.environ.get("MICTLANX_PASSWORD"), expires_in = os.environ.get("MICTLANX_EXPIRES_IN","1d") )
    with open(path,"rb") as f:
        data  = f.read()
        metadata = {}
        payload = PutPayload(key=key,bytes = data,metadata=metadata)
        res = c.put(payload)
        print(res)
import sys
import os
from mictlanx.v3.services.xolo import Xolo
from dotenv import load_dotenv
import requests as R
from mictlanx.v3.interfaces.payloads import PutPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.services.replica_manger import ReplicaManager
from dotenv import load_dotenv
from option import Some,NONE

load_dotenv()
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
if __name__ =="__main__":
    args      = sys.argv[1:]
    if(len(args) >= 5  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v3/05_xolo_aes.py <KEY> <SECRET> <PATH> <DEFAULT>")
    key        = args[0]
    secret     = args[1]
    path       = args[2]
    is_default = args[3] == "default"
    
    # _________________________
    # 1. Create an instance of Xolo.
    secret_key            = bytes.fromhex(secret)
    if not is_default:
        with open(path,"rb") as f :
            data = f.read()
    else:
        data = b"Hello world"
    encrypted_data_result = xolo.encrypt_aes(key=secret_key,data=data)
    # print("ENCRYPTED_DATA",encrypted_data_result)
    if(encrypted_data_result.is_ok):
        encrypted_data  = bytearray(encrypted_data_result.unwrap())
        put_response = c.put(key=key,value=encrypted_data,group_id=key,content_type=Some("application/octet-stream"),update=True)
        print("PUT_RESPONSE",put_response)
        get_response = c.get(key=key)
        plaintext      = xolo.decrypt_aes(key=secret_key, data=get_response.unwrap().value )
        print("DECRYPTED_DATA",plaintext)
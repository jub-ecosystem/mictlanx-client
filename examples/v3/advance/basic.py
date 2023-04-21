import pandas as pd
import os
from mictlanx.v3.interfaces.payloads import PutPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.auth import Auth
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.interfaces.payloads import GenerateTokenPayload
from mictlanx.v3.services.replica_manger import ReplicaManager
from nanoid import generate as nanoid_
from requests_toolbelt import MultipartEncoder
import hashlib as H
import magic as M
import requests as R
import json as J



xel_trace = pd.read_csv("/home/nacho/Programming/Python/mictlanx/examples/v3/fake_trace.csv")
# xel_trace = pd.read_csv("/home/nacho/Programming/Python/mictlanx/examples/v3/xel_trace.csv")
# rm_service   = ReplicaManager(ip_addr = "gamma.tamps.cinvestav.mx", port=20000, api_version=3)
# auth_service = Auth(ip_addr = "gamma.tamps.cinvestav.mx", port=10000, api_version=3)
# c            = Client(rm_service = rm_service, auth_service = auth_service,password = "t0p53cR3T#")

def simple_put(path:str):
    with open(path,"rb") as f:
        data = f.read()
        content_type = M.from_file(path,mime=True)
        metadata ={ "size":str(len(data)),"content_type":content_type}
        payload = PutPayload(key =nanoid_(),bytes = data,metadata = metadata)
        print(payload,metadata)
        x = c.put(payload)
        print("CLIENT_PUT_RESULT",x)
        # if(x.is_err):
            # e = x.unwrap_err()
            # print(e)

# paths = ["/home/nacho/Programming/Python/mictlanx/examples/data/yamerito.jpg","/home/nacho/Programming/Python/mictlanx/examples/data/burrito.gif"]
# for  path in paths :
#     simple_put(path)
    # data1 = open("","rb")
    
    # bytes_1 = data0.read()
def put_from_trace():
    for (index,row) in list(xel_trace.iterrows())[1:]:
        chimistreta_path = row["CHIMISTRETA_PATH"]
        with open(chimistreta_path,"rb") as f:
            data = f.read()
            content_type = M.from_file(chimistreta_path,mime=True)
            key = row["KEY"]
            parent = row["PARENT"]
            child = row["CHILD"]
            if not (type(parent) == str):
                parent = "/"
            product_type = row["PRODUCT_TYPE"]
            extension =row["EXTENSION"]
            size = row["SIZE"]
            # checksum = row["HASH"]
            metadata = {
                "parent":parent, "product_type":product_type,"extension":extension,
                "size":str(size),"child":child,"content_type":content_type
            }
            # print(metadata)
            payload = PutPayload(key =key,bytes = data,metadata = metadata)
            put_res = c.put(payload)
            print("PAYLOAD[{}]".format(index),payload,put_res,metadata)
            # i+=1
            
def put_from_trace_proxy():
    auth_payload = GenerateTokenPayload(password="top_secret",expires_in = 7200)
    auth_service = Auth(ip_addr = "localhost", port=10000, api_version=3)
    auth = auth_service.generate_token(auth_payload)
    print("AUTH_RESPONSE",auth)
    for (index,row) in list(xel_trace.iterrows()):
        chimistreta_path = row["CHIMISTRETA_PATH"]
        with open(chimistreta_path,"rb") as f:
            data = f.read()
            content_type = M.from_file(chimistreta_path,mime=True)
            key = row["KEY"]
            parent = row["PARENT"]
            child = row["CHILD"]
            if not (type(parent) == str):
                parent = "/"
            product_type = row["PRODUCT_TYPE"]
            extension =row["EXTENSION"]
            size = len(data)
            # row["SIZE"]
            # checksum = row["HASH"]
            metadata = {
                "parent":parent, "product_type":product_type,"extension":extension,
                "size":str(size),"child":child,"content_type":content_type
            }
            rm_put_payload = {
                "ball_id":key,
                "ball_size":size,
                "metadata": metadata,
            }
            # files = {[key]:{[key]:data }, "metadata":{} }
            metadata_json=J.dumps(rm_put_payload)

            print("METADATA_JSON",metadata_json)
            fields = {
                "metadata":(None, metadata_json,"application/json"),
                key:(key,data, "application/octet-stream")
            }
            mp_encoder = MultipartEncoder(fields=fields)
            # print("FIELDS",mp_encoder.fields)
            print("MP_HEADERS",mp_encoder.content_type)
            proxy_put_res = R.post(
                "http://localhost:8080/api/v3",
                files = fields,
                headers={
                    # "Content-Type":mp_encoder.content_type,
                    "Client-Id":auth.client_id,
                    "Authorization":auth.token,
                    "Password":"top_secret"
                }
            )
                                #    files=mp_encoder)
            print("PROXY_PUT_RES",proxy_put_res.headers)
            print("PROXY_PUT_RES",proxy_put_res.status_code)
            print("PROXY_PUT_RES",proxy_put_res.reason)
            print("_"*40)
            # print(metadata)
            # payload = PutPayload(key =key,bytes = data,metadata = metadata)
            # put_res = c.put(payload)
            # print("PAYLOAD[{}]".format(index),payload,put_res,metadata)


        

if __name__ == "__main__":
    # put_from_trace_proxy()
    rm_service   = ReplicaManager(ip_addr = "localhost", port=20000, api_version=3)
    auth_service = Auth(ip_addr = "localhost", port=10000, api_version=3)
    proxy_service = Proxy(ip_addr = "localhost", port=8080, api_version=3)
    c            = Client(rm_service = rm_service, auth_service = auth_service,proxy = proxy_service,password = "t0p53cR3T#")
    with open("/source/01.pdf","rb") as f:
        data  = f.read()
        metadata = {
            "tag_1":"VALUE",
            "tag_2":"1000",
            "tag_3":J.dumps({"tag1":"VALUE","tag2":"2"})
        }
        payload = PutPayload(key="ball-0",bytes = data,metadata=metadata)
        res = c.put(payload)
        print(res)
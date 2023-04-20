import pandas as pd
import numpy as np
import os
from mictlanx.v3.interfaces.payloads import PutPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.auth import Auth
from mictlanx.v3.services.replica_manger import ReplicaManager
from nanoid import generate as nanoid_
import hashlib as H
import magic as M



xel_trace = pd.read_csv("/home/nacho/Programming/Python/mictlanx/examples/v3/xel_trace.csv")
rm_service   = ReplicaManager(ip_addr = "gamma.tamps.cinvestav.mx", port=20000, api_version=3)
auth_service = Auth(ip_addr = "gamma.tamps.cinvestav.mx", port=10000, api_version=3)
c            = Client(rm_service = rm_service, auth_service = auth_service,password = "t0p53cR3T#")

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
    put_limit = 10
    for (index,row) in xel_trace.iterrows():
        if(index == put_limit):
            break
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
            i+=1
            


        


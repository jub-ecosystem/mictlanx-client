import pandas as pd
import numpy as np
import os
from mictlanx.v3.interfaces.payloads import PutPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.auth import Auth
from mictlanx.v3.services.replica_manger import ReplicaManager



xel_trace = pd.read_csv("/home/nacho/Programming/Python/mictlanx/examples/v3/xel_trace.csv")
rm_service   = ReplicaManager(ip_addr = "alpha.tamps.cinvestav.mx", port=20000, api_version=1)
auth_service = Auth(ip_addr = "alpha.tamps.cinvestav.mx", port=10000, api_version=1)
c            = Client(rm_service = rm_service, auth_service = auth_service,password = "t0p53cR3T#")

for (index,row) in xel_trace.iterrows():
    key = row["KEY"]
    parent = row["PARENT"]
    child = row["CHILD"]
    if not (type(parent) == str):
        parent = "/"
    product_type = row["PRODUCT_TYPE"]
    extension =row["EXTENSION"]
    size = row["SIZE"]
    checksum = row["HASH"]
    metadata = {
        "parent":parent, "product_type":product_type,"extension":extension,
        "size":str(size),"checksum":checksum,"child":child

    }
    print(metadata)
    payload = PutPayload(key =key,bytes = b"FAKE",metadata = metadata)
    mimetype = 
    print("PAYLOAD",payload)
    
    # put_res = c.put(payload)
    # print(put_res)



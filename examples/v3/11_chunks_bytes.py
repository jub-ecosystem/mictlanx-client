import sys
import os
import pandas as pd
from mictlanx.v3.interfaces.payloads import PutPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.services.replica_manger import ReplicaManager
from mictlanx.utils.segmentation import Chunks,Chunk
from dotenv import load_dotenv
from option import Some,NONE
import numpy as np

load_dotenv()
if __name__ =="__main__":
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v3/01_put.py <KEY> <PATH>")
    key = args[0]
    path = args[1]
    replica_manager  = ReplicaManager(ip_addr = os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"), port=int(os.environ.get("MICTLANX_REPLICA_MANAGER_PORT",20000)), api_version=Some(3))
    xolo             = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port=int(os.environ.get("MICTLANX_XOLO_PORT",10000)), api_version=Some(3))
    proxy            = Proxy(ip_addr = os.environ.get("MICTLANX_PROXY_IP_ADDR"), port=int(os.environ.get("MICTLANX_PROXY_PORT",8080)), api_version=Some(3))
    secret           = os.environ.get("MICTLANX_SECRET")
    expires_in       = os.environ.get("MICTLANX_EXPIRES_IN","1d")
    app_id           = os.environ.get("MICTLANX_APP_ID")
    
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
    # ndarray         = pd.read_csv(path,header=None).values
    for i in range(100):
        ndarray = np.random.randint(0,100, size=(10000,10))
        num_chunks = np.random.randint(1,4)
        chunks          =  list(map(lambda chunk:chunk.data ,Chunks.from_ndarray(ndarray=ndarray,group_id="group-{}".format(i),chunk_prefix=NONE,num_chunks=num_chunks).unwrap().to_list()))
        response = c.put_chunks_bytes(
            key="{}_{}".format(key,i),
            chunks=chunks,
            tags={"tags":"value_Examle"},
            group_id="group_{}".format(i),
            storage_node_id=NONE,
            replica_manager_id=NONE,
            content_type=NONE,
            cache=False,
            update=True,
        )
        print(response)
        print("_"*20)


    # with open(path,"rb") as f:
        # data     = f.read()
    # result = c.put_ndarray_chunks(
    #     ndarray    = ndarray,
    #     group_id   = key,
    #     num_chunks = 3,
    #     tags={
    #         "tag_1":"VALUE"
    #     },
    #     update=True
    # )
    # print(result)
    # for i in range(2):
    #     # get_res = c.get_chunked(key=key,cache=True)
    #     get_res = c.get_chunked_ndarray(key=key)
    #     if get_res.is_ok:
    #         response = get_res.unwrap()
    #         # print(response.value)
    #         # print(response.value.shape)
    #         print(response.metadata.checksum)
    #         print("_"*20)
    #         # response.value
    #     # print("GET_RES",get_res)
    #     print("_"*20)
    #     # print("RESULT",result)
    c.logout()
        # metadata = {}
        # payload  = PutPayload(key=key,data = data,metadata=metadata)
        # res      = c.put(
        #     key="ball_0",
        #     value = data,
        #     tags={
        #         "tag_1":"VALUE_1",
        #         "tag_2":"VALUE_2",
        #     },
        #     group_id="group_0"
        #     # metadata=
        # )
        # print(res)
    c.logout()

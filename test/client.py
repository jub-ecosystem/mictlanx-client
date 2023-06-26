import os
import unittest
import magic as M
import numpy as np
import json as J 
from option import Option 
from option import Some,NONE
import hashlib as H
import time as T
from concurrent.futures import ThreadPoolExecutor,as_completed
from mictlanx.v3.client  import Client
from mictlanx.v3.services.replica_manger import ReplicaManager
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.interfaces.xolo import XoloCredentials
from mictlanx.utils.segmentation import Chunks
from mictlanx.v3.interfaces.payloads import LogoutPayload,PutMetadataPayload
from dotenv import load_dotenv
load_dotenv()

# credentials = XoloCredentials(application_id="APP_ID", client_id="CLIENT_ID", secret="SECRET", authorization="AUTHORIZATION")
replica_manager  = ReplicaManager(ip_addr = os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"), port=int(os.environ.get("MICTLANX_REPLICA_MANAGER_PORT",20000)), api_version=Some(3))
xolo             = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port=int(os.environ.get("MICTLANX_XOLO_PORT",10000)), api_version=Some(3))
proxy            = Proxy(ip_addr = os.environ.get("MICTLANX_PROXY_IP_ADDR"), port=int(os.environ.get("MICTLANX_PROXY_PORT",8080)), api_version=Some(3))
app_id           = os.environ.get("MICTLANX_APP_ID")
client_id        = os.environ.get("MICTLANX_CLIENT_ID")
secret           = os.environ.get("MICTLANX_SECRET")
expires_in       = os.environ.get("MICTLANX_EXPIRES_IN","1d")

c             = Client(
    app_id=app_id,
    client_id=Some(client_id),
    metadata=NONE,
    replica_manager=replica_manager,
    xolo = xolo,
    proxies=[proxy],
    secret=secret,
    expires_in=Some(expires_in)
)
class ClientTestSuite(unittest.TestCase):
    @unittest.skip("A")
    def test_put(self):
        result = c.put(
            key="b1",
            value = b"HOLA",
            tags={
                "k1":"VALUE_1",
                "k2":"VALUE_2",
                "k3":"VALUE_3",
            }
        )
        print("PUT_RESULT",result)
        logout_payload = LogoutPayload(app_id=app_id,client_id=c.credentials.client_id,token=c.credentials.authorization,secret=c.credentials.secret)
        # 
        logout_res = c.xolo.logout(payload= logout_payload)
        print("LOGOUT_RESULT",logout_res)
    @unittest.skip("A")
    def test_put_datasets(self):
        base_filename = "CLUSTERING_C1_50_R10_A10_K3"
        base_path = "/source/datasets"
        RF = 3
        for i in range(50):
            filename = "{}_{}".format(base_filename,i)
            path = "{}/{}.csv".format(base_path,filename)
            with open(path,"rb") as f:
                data = f.read()
                # print("FILE_SIZE",len(data))
                for j in range(RF):
                    result = c.put_with_checksum_as_key(
                        value=data,
                        tags={
                            "tag_1":"VALUE_0",
                            "tag_2":"VALUE_1",
                        }
                    )
                    if result.is_err:
                        print("ERROR TO UPLOAD REPLICA_{} DATASET".format(j),i,result.unwrap_err())
                        continue
                    print("DATASET UPLOADED",i)
                # T.sleep(2)
        c.logout()
                



    @unittest.skip("A")
    def test_put_ndarrays(self):
        RF = 1
        for i in range(10):
            a = np.random.randint(low= 10, high=1000)
            cols = np.random.randint(low= 1, high=1000)
            data = np.random.random(size=(a,cols))
            for j in range(RF):
                d = data.tobytes()
                h = H.sha256()
                h.update(d)
                cs = h.hexdigest()
                # print("CHECKSUM_CLIENT",cs)
                key = "matrix-{}".format(0)
                result = c.put_ndarray(
                    key= key,
                    ndarray=data,
                    tags={
                    },
                    update=True
                )
                if result.is_err:
                    print("ERROR TO UPLOAD REPLICA_{} DATASET".format(j),i,result.unwrap_err())
                    continue
                print("KEY",key)
                print("CHECKSUM",cs)
                print("_"*30)
                # print("DATASET {} /  REPLICA {}".format(i,j))
                T.sleep(5)
        #         # T.sleep(2)
        c.logout()
    

    @unittest.skip("A")
    def test_get(self):
        total_gets = 200
        try:
            def get(*args,**kwargs):
                try:
                    key = "matrix-{}".format(0)
                    # .format(np.random.randint(low=0,high=10))
                    # print("ARGS",args,"KEY",key)
                    # res = c.get(key=key )
                    res = c.get_ndarray(key=key).unwrap()
                    d = res.value.tobytes()
                    h = H.sha256()
                    h.update(d)
                    cs = h.hexdigest()
                    print(res)
                    print("CHECKSUM",cs)
                    print("_"*30)
                    # T.sleep(1)
                    return res
                    # return
                except Exception as e:
                    print("ERROR",e)

            
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = [ executor.submit(get,args=(i,),kwargs={}) for i in range(total_gets) ]
                for r in as_completed(results):
                    print("RESULT",r.result())

                # executor.shutdown(wait=True)
            c.logout()
            
        except Exception as e:
            print("ERROR 2",e)

    @unittest.skip("A")
    def test_put_ndarray(self):
        result = c.put_ndarray(key="encrypted_matrix-0",ndarray=np.ones((10,100)),tags={"desx":"A MATRIX OF ONES"})
        print("PUT_NDARRAY_RESULT",result)
    
    @unittest.skip("A")
    def test_put_ndarray_segmentation(self):
        ndarray = np.random.random(size=(10,10))
        result  = c.put_ndarray_chunks(key="grouped_matrix",ndarray=ndarray,group_id="grouped_matrix",num_chunks=3)
        print("RESULT {}", result)
    def test_get_chunked(self):
        group_id = "grouped_matrix"
        result = c.get_chunked(key=group_id)
        print(result)
        c.logout()
    @unittest.skip("A")
    def test_get_metadata(self):
        key    = "grouped_matrix_0"
        result = c.get_metadata(key=key)
        if result.is_ok:
            print(result.unwrap())
        c.logout()



if __name__ =="__main__":
    unittest.main()
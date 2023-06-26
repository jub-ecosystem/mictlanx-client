import unittest 

from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.interfaces.payloads import PutMetadataPayload
from mictlanx.v3.interfaces.xolo import XoloCredentials
import json as J 
from option import Some,NONE
import hashlib as H
import time as T
from concurrent.futures import ThreadPoolExecutor
import numpy as np
# from hashlib import 
import magic as M

from mictlanx.utils.segmentation import Chunks
from option import Option 

credentials = XoloCredentials(application_id="APP_ID", client_id="CLIENT_ID", secret="SECRET", authorization="AUTHORIZATION")
proxy    = Proxy(ip_addr="localhost",port=8080, api_version=Some(3))
class ProxyTestSuite(unittest.TestCase):
    
    def test_put_metadata(self):
        # maybe_chunks:Option[Chunks] = Chunks.from_file(path="/source/01.pdf",group_id="test",workers=2)
        maybe_chunks:Option[Chunks] = Chunks.from_ndarray(ndarray=np.ones((100,140)),group_id="encrypted_matrix_0",num_chunks=2)
        if maybe_chunks.is_some:
            chunks = maybe_chunks.unwrap()
            for chunk in chunks.chunks:
                print("SEND_CHUNK_TO_STORAGE",str(chunk.checksum))
                result= proxy.put(
                        key=Some("encrypted_matrix-"+str(chunk.index)),
                        value= chunk.data,
                        group_id=chunk.group_id,
                        metadata= {
                            **chunk.metadata,
                            "content_type": M.from_buffer(chunk.data, mime=True)
                        },
                        headers= credentials.to_headers()
                    )
                print("RESULT",result)



    @unittest.skip("A")
    def test_put(self):
        def f(*args):
            prefix = args[0]+"_"
            hasher = H.sha256()
            rf = 10
            
            with open("/source/01.pdf","rb") as f:
                data = f.read()
                for i in range(0,rf): 
                    result= proxy.put(
                        key = Some(prefix+str(i)),
                        value= data,
                        group_id=prefix,
                        metadata= {
                            "product_type":"VALUE",
                            "title":"TITLE",
                            "desc":"DESCRIPTION",
                            "content_type": M.from_buffer(data, mime=True)
                        },
                        headers= credentials.to_headers()
                    )
                    print("RESULT",result)
                    interarrival_time = np.random.exponential(scale=.5)
                    print("INTERARRIVAL_TIME", interarrival_time)
                    T.sleep(interarrival_time)

        with ThreadPoolExecutor(max_workers=5) as executor:
                executor.map(f, ["PRODUCT","DATASET","MAP","MATRIX"])
    @unittest.skip("A")
    def test_get(self):
        concurrency_level = 5
        with ThreadPoolExecutor(max_workers= concurrency_level) as executor:
            counter = 10
            for i in range(counter):
                result = proxy.get(key="b1", headers=credentials.to_headers())
                print("RESULT",i, result)

if __name__ =="__main__":
    unittest.main()
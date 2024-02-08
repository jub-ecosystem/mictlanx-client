
import unittest as UT
import os
import time as T
from typing import List,Awaitable
from mictlanx.v4.client import Client
from option import Result
from mictlanx.v4.interfaces.responses import PutResponse,GetBytesResponse
from mictlanx.utils.index import Utils
from concurrent.futures import as_completed

peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000")) 
c = Client(
    client_id   = "client-example-0",
    peers       = list(peers),
    debug       = True,
    daemon      = True, 
    show_metrics= False,
    max_workers = 6,
    lb_algorithm="2CHOICES_UF"
)
class MictlanXTestSuite(UT.TestCase):
    @UT.skip("")
    def test_01_put(self):
        path = "/source/01.pdf"
        with open(path,"rb") as f:
            result                          = c.put(value=f.read(),tags={"example_name":"01_put"})
            x:Result[PutResponse,Exception] = result.result()
            assert x.is_ok
    
    def test_02_get(self):
        futures:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
        key = "38532d11446c55c07fadc1db2511c9c16146877d491a7472b6203c1ad62fbd0c"
        num_gets = 10
        for i in range(num_gets):
            future = c.get(key=key)
            print(i,future)
            futures.append(future)
        
        success= 0
        for i,future in enumerate(as_completed(futures)):
            try:
                result = future.result()
                print(i,result)
                if result.is_ok:
                    success +=1
            except Exception as e:
                print(i,e)
        assert success == num_gets
    


    

if __name__ == "__main__":
    UT.main(failfast=True, exit=True)

import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import GetBytesResponse
from concurrent.futures import as_completed
from typing import List,Awaitable
from option import Result
from mictlanx.utils.index import Utils
import dotenv 
dotenv.load_dotenv()

    

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/02_get.py <KEY> <NUM_DOWNLOADS>")
    key          = args[0]
    num_downloas = 1 if len(args) == 1 else int(args[1])
    peers        =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    client            = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug        = True,
        daemon       = True, 
        show_metrics = False,
        max_workers  = 1,
        lb_algorithm = "2CHOICES_UF",
        log_when     = "s",
        log_interval = 120,
        bucket_id    = "B1"
    )

    futures:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
    for i in range(num_downloas):
        future = client.get(bucket_id="B3",key=key)
        
        print(i,future)
        futures.append(future)
    
    for i,future in enumerate(as_completed(futures)):
        result = future.result()
        print(i,result)
        T.sleep(1)
    
    T.sleep(10)

if __name__ == "__main__":
    example_run()
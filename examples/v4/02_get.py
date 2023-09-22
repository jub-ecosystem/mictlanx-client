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
    c            = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True, 
        max_workers = 4
    )

    futures:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
    for i in range(num_downloas):
        future = c.get(key=key)
        futures.append(future)
        # T.sleep(1)
    
    for future in as_completed(futures):
        result = future.result()
        print(result)
    
    T.sleep(20)

if __name__ == "__main__":
    example_run()
import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import GetBytesResponse
from mictlanx.utils.index import Utils
from concurrent.futures import as_completed
from typing import List,Awaitable
from option import Result
import dotenv 
dotenv.load_dotenv()


def example_run():
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/04_get_ndarray.py <KEY> <NUM_DOWNLOADS>")
    key = args[0]
    num_downloas = 1 if len(args) == 1 else int(args[1])
    peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000")) 
    c = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True,
        show_metrics=False, 
        max_workers = 1,
        lb_algorithm="2CHOICES_UF"
    )
    futures:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
    for i in range(num_downloas):
        future =  c.get_ndarray(key=key)
        # print("FUTURE",future)
        futures.append(future)
        # T.sleep(1)
    
    for future in as_completed(futures):
        result = future.result()
        print("RESULT",result)
    
    # T.sleep(20)

if __name__ == "__main__":
    example_run()
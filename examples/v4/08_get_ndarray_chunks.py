import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.index import Peer
from mictlanx.v4.interfaces.responses import GetBytesResponse
from concurrent.futures import as_completed
from typing import List,Awaitable
from option import Result
import dotenv 
dotenv.load_dotenv()

def peers_from_str(peers_str:str,separator:str=" ")->List[Peer]:
    splitted = peers_str.split(separator)
    splitted = map(lambda x: x.split(":"), splitted)
    return map(lambda x: Peer(node_id=x[0],ip_addr=x[1], port=int(x[2])), splitted)
    

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/02_get.py <KEY> <NUM_DOWNLOADS>")
    key          = args[0]
    num_downloas = 1 if len(args) == 1 else int(args[1])
    peers =  peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    c = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True, 
        max_workers = 4
    )
    for i in range(num_downloas):
        res = c.get_and_merge_ndarray(key=key)
        print("RESULT[{}]".format(i),res.result().unwrap())
    # futures:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
    # for i in range(num_downloas):
    #     future = c.get(key=key)
    #     futures.append(future)
    #     T.sleep(.5)
    
    # for future in as_completed(futures):
    #     result = future.result()
    #     print(result)
    
    # T.sleep(20)

if __name__ == "__main__":
    example_run()
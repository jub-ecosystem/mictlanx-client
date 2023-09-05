import os
import sys
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.index import Peer
from mictlanx.v4.interfaces.responses import PutResponse
from typing import List
from option import NONE, Result,Some,Option
import dotenv 
dotenv.load_dotenv()

def peers_from_str(peers_str:str,separator:str=" ")->List[Peer]:
    splitted = peers_str.split(separator)
    splitted = map(lambda x: x.split(":"), splitted)
    return map(lambda x: Peer(node_id=x[0],ip_addr=x[1], port=int(x[2])), splitted)
    

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 2  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/01_put.py <PATH>")
    path = args[0]
    # path = args[1]
    peers =  peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    c = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True, 
        max_workers = 2
    )
    # path = 
    with open(path,"rb") as f:
        result                          = c.put(value=f.read(),tags={"example_name":"01_put"})
        x:Result[PutResponse,Exception] = result.result()
        print(x)


if __name__ == "__main__":
    example_run()
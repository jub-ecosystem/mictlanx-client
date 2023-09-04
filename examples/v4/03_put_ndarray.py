import os
import sys
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.index import Peer
from mictlanx.v4.interfaces.responses import PutResponse
from typing import List
from option import NONE, Result,Some,Option
import numpy as np
import dotenv 
dotenv.load_dotenv()

def peers_from_str(peers_str:str,separator:str=" ")->List[Peer]:
    splitted = peers_str.split(separator)
    splitted = map(lambda x: x.split(":"), splitted)
    return map(lambda x: Peer(node_id=x[0],ip_addr=x[1], port=int(x[2])), splitted)
    

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 4  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/03_put_ndarray.py <KEY> <ROWS> <COLUMNS>")
    key  = args[0].strip()
    rows = int(args[1])
    cols = int(args[2])
    peers =  peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    c = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True, 
        max_workers = 2
    )
    ndarray = np.random.random(size=(rows,cols))
    x       = c.put_ndarray(key=key,ndarray=ndarray,tags={"example_id":"03_put_ndarray.py"})
    print(x.result())

if __name__ == "__main__":
    example_run()
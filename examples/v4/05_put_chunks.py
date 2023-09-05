import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.index import Peer
from mictlanx.v4.interfaces.responses import PutResponse
from mictlanx.utils.segmentation import Chunks,Chunk
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
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/05_put_chunks.py <KEY> <PATH>")
    key   = args[0]
    path  = args[1]
    peers =  peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    c     = Client(
            client_id   = "client-example-0",
            peers       = list(peers),
            debug       = True,
            daemon      = True, 
            max_workers = 2
    )
    maybe_chunks = Chunks.from_file(path=path, group_id=key, num_chunks=5)
    if maybe_chunks.is_none:
        raise "something went wrong creating the chunks"
    result = c.put_chunks(key=key,chunks=maybe_chunks.unwrap(),tags={"example_name":"05_put_chunks"})
    for res in result:
        print(res)
        # x:Result[PutResponse,Exception] = result.result()
        # print(x)


if __name__ == "__main__":
    example_run()
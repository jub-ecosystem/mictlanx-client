import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.utils.segmentation import Chunks
from mictlanx.utils.index import Utils
import dotenv 
import numpy as np
dotenv.load_dotenv()

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 5  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/06_put_ndarray_chunks.py <KEY> <CHUNKS> <ROWS> <COLS>")
    key        = args[0]
    num_chunks = int(args[1])
    rows       = int(args[2])
    cols       = int(args[3])
    peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    
    c     = Client(
            client_id   = "client-example-0",
            peers       = list(peers),
            debug       = True,
            daemon      = True, 
            max_workers = 2,
            lb_algorithm="2CHOICES_UF"
    )
    
    # 1. matrix
    np.random.seed(42)
    encrypted_matrix      = np.random.random(size=(rows,cols,10))
    # 2. chunks
    maybe_chunks = Chunks.from_ndarray(ndarray=encrypted_matrix,group_id=key, num_chunks=num_chunks)
    if maybe_chunks.is_none:
        raise "something went wrong creating the chunks"
    # 3. store the chunks
    result = c.put_chunks(key=key,chunks=maybe_chunks.unwrap(),tags={"example_name":"05_put_chunks"})
    
    for res in result:
        print(res)
        T.sleep(2)
    # T.sleep(100)
        # x:Result[PutResponse,Exception] = result.result()
        # print(x)


if __name__ == "__main__":
    example_run()
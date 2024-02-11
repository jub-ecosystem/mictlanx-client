import os
import sys
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
import numpy as np
import dotenv 
dotenv.load_dotenv()

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 4  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/03_put_ndarray.py <KEY> <ROWS> <COLUMNS>")
    key  = args[0].strip()
    rows = int(args[1])
    cols = int(args[2])
    peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    bucket_id = "rory"
    c = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True, 
        max_workers = 2,
        lb_algorithm="2CHOICES_UF",
        bucket_id=bucket_id
    )
    ndarray = np.random.random(size=(rows,cols,102))
    x       = c.put_ndarray(
        key=key,
        ndarray=ndarray,
        tags={"example_id":"03_put_ndarray.py"}, 
    )
    print(x.result())

if __name__ == "__main__":
    example_run()
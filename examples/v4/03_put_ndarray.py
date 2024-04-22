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

    routers        =  list(Utils.routers_from_str(routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")))
    # peers =  Utils.routers_from_str(routers_str=)
    # .peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    bucket_id = "rory"
    c = Client(
        client_id   = "client-example-0",
        routers=routers,
        debug       = True,
        max_workers = 2,
        bucket_id=bucket_id
    )
    np.random.seed(10)
    ndarray = np.random.random(size=(rows,cols,102))
    x       = c.put_ndarray(
        key=key,
        ndarray=ndarray,
        tags={"example_id":"03_put_ndarray.py"}, 
    )
    print(x.result())

if __name__ == "__main__":
    example_run()
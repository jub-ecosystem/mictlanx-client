import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.utils.segmentation import Chunks
import dotenv 
dotenv.load_dotenv()

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/05_put_chunks.py <KEY> <PATH>")
    key   = args[0]
    path  = args[1]
    routers        =  list(Utils.routers_from_str(routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")))
    c     = Client(
            client_id   = "client-example-0",
            routers     = list(routers),
            debug       = True,
            daemon      = True, 
            max_workers = 1,
            bucket_id="mictlanx"
    )
    maybe_chunks = Chunks.from_file(path=path, group_id=key, num_chunks=5)
    if maybe_chunks.is_none:
        raise "something went wrong creating the chunks"
    result = c.put_chunks(
        key    = key,
        chunks = maybe_chunks.unwrap(),
        tags   = {"example_name":"05_put_chunks"}
    )
    for res in result:
        print(res)

if __name__ == "__main__":
    example_run()
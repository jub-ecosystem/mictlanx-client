import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import GetBytesResponse
from concurrent.futures import as_completed
from typing import List,Awaitable
from option import Result,Some,NONE
from mictlanx.utils.index import Utils
import dotenv 
dotenv.load_dotenv()

    

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 5  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/02_get.py <BUCKET_ID> <KEY> <OUTPUT_PATH> <FILENAME>")
    routers        =  Utils.routers_from_str(routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"))
    bucket_id = Utils.get_or_default(iterator=args, i = 0, default="mictlanx").unwrap()
    key          = Utils.get_or_default(iterator=args,i=1).unwrap_or("INSERT_A_KEY")
    output_path     = Utils.get_or_default(iterator=args,i=2,default="/mictlanx/data").unwrap_or("/mictlanx/data")
    filename     = Utils.get_or_default(iterator=args,i=3,default="").unwrap_or("")
                        # self.__keys_per_peer[metadata_response.node_id] = set([key])
    # num_d = int(Utils.get_or_default(iterator=args,i=2,default=1).unwrap())
    # 1 if len(args) == 1 else int(args[1])
    client            = Client(
        client_id   = "client-example-0",
        routers       = list(routers),
        debug        = True,
        max_workers  = 1,
        log_when     = "m",
        log_interval = 20,
        bucket_id    = bucket_id,
        log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
    )

    # for i in range(100):
    result = client.get(
        bucket_id=bucket_id,
        key=key,
        chunk_size="1MB",
        headers={
            "Consistency-Model":"STRONG" # Default: LB
        }
    )
        # output_path=output_path,
        # filename=filename,
    if result.is_ok:
        print(result.unwrap().metadata.tags)
    # print("Get[{}]".format(i),result)

if __name__ == "__main__":
    example_run()
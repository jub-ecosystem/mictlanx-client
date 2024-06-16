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
    if(len(args) >= 4  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/02_get.py <BUCKET_ID> <KEY> <NUM_DOWNLOADS>")
    bucket_id = Utils.get_or_default(iterator=args, i = 0, default="mictlanx").unwrap()
    key          = Utils.get_or_default(iterator=args,i=1).unwrap_or("INSERT_A_KEY")
    num_downloas = int(Utils.get_or_default(iterator=args,i=2,default=1).unwrap())
    # 1 if len(args) == 1 else int(args[1])
    routers        =  list(Utils.routers_from_str(routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")))
    client            = Client(
        client_id   = "mictlanx",
        routers=routers,
        debug        = True,
        max_workers  = 1,
        lb_algorithm = "2CHOICES_UF",
        log_when     = "s",
        log_interval = 120,
        bucket_id    = bucket_id
    )

    # futures:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
    for i in range(num_downloas):
        result = client.get_with_retry(bucket_id=bucket_id,key=key )
        print("GET_RESULT",result)
    # futures.append(future)
    
    # for i,future in enumerate(as_completed(futures)):
        # result = future.result()

if __name__ == "__main__":
    example_run()
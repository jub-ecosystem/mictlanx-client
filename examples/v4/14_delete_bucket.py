import os
import sys
import time as T
from mictlanx.v4.client import Client
from concurrent.futures import as_completed
from option import Result,Some,NONE
from mictlanx.utils.index import Utils
import dotenv 
dotenv.load_dotenv()

    

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 2  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/14_delete_bucket.py <BUCKET_ID>")
    routers        =  Utils.routers_from_str(routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"))
    bucket_id = Utils.get_or_default(iterator=args, i = 0, default="mictlanx").unwrap()
    client            = Client(
        client_id   = "client-example-0",
        routers       = list(routers),
        debug        = True,
        max_workers  = 5,
        log_when     = "m",
        log_interval = 20,
        bucket_id    = bucket_id,
        log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
    )

    res = client.delete_bucket_async(bucket_id=bucket_id)
    
    print(res)
    # for metadata in client.get_all_bucket_metadata(bucket_id=bucket_id):
    #     for ball in metadata.balls:
    #         start_time = T.time()
    #         del_result = client.delete(key=ball.key,bucket_id=bucket_id)
    #         if del_result.is_ok:
    #             response_time = T.time() - start_time
    #             print("delete {} SUCCESSFULLY - {}".format(ball.key, response_time))
    #         else:
    #             print("DELETE {} FAILED".format(ball.key))


if __name__ == "__main__":
    example_run()
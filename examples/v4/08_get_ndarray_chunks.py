import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
import dotenv 
dotenv.load_dotenv()


def example_run():
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/08_get_ndarray_chunks.py <KEY> <NUM_DOWNLOADS>")
    key          = args[0]
    num_downloas = 1 if len(args) == 1 else int(args[1])
    peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    bucket_id = "rory"
    c = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True, 
        max_workers = 2,
        lb_algorithm="2CHOICES_UF",
        bucket_id=bucket_id,
        show_metrics=False
    )
    for i in range(num_downloas):
        res = c.get_and_merge_ndarray(key=key).result()
        if res.is_err:
            print("Error {}".format(res.unwrap_err()))
        else:
            print(res.unwrap().value.shape)

if __name__ == "__main__":
    example_run()
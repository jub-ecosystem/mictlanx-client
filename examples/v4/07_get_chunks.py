import os
import sys
import time as T
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
import dotenv 
dotenv.load_dotenv()

# SORTED_CHUNK_INDEX 0 b38dae33c00290376f8f105fd36babe9e21870009c08b661fd1822ac360153ec
# SORTED_CHUNK_INDEX 1 e645ca5ae5ac747486bb0d18ebdc8721174ee414777c7cb051c3304574db1d6b
# SORTED_CHUNK_INDEX 2 9dfce525fd06dd8dcc0d616b4222bb5092a6e0dba60e82f91cb16dc39d845d54

def example_run():
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/07_get_chunks.py <KEY> <NUM_DOWNLOADS>")
    key          = args[0]
    num_downloas = 1 if len(args) == 1 else int(args[1])

    routers        =  list(Utils.routers_from_str(routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")))
    bucket_id = "mictlanx"
    c = Client(
        client_id    = "client-example-0",
        routers=routers,
        debug        = True,
        max_workers  = 2,
        bucket_id=bucket_id
    )
    for i in range(num_downloas):
        res = c.get_and_merge(key=key)
        result = res.result()
        if result.is_ok:
            print("RESULT[{}]".format(i),result.unwrap().metadata)
        else:
            print(i,"ERROR")
    # T.sleep(100)
    

if __name__ == "__main__":
    example_run()
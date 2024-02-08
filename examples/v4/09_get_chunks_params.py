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
    if(len(args) >= 4  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/02_get.py <KEY> <NUM_CHUNKS> <NUM_DOWNLOADS>")
    key          = args[0]
    num_chunks   = int(args[1])
    num_downloas = 1 if len(args) <= 2 else int(args[2])
    peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    bucket_id = "b0"
    c = Client(
        client_id    = "client-example-0",
        peers        = list(peers),
        debug        = True,
        daemon       = True, 
        show_metrics = False,
        max_workers  = 2,
        lb_algorithm="2CHOICES_UF",
        bucket_id=bucket_id
    )
    for i in range(num_downloas):
        res = c.get_and_merge_with_num_chunks(key=key,num_chunks=num_chunks)
        result = res.result()
        if result.is_ok:
            print("RESULT[{}]".format(i),result.unwrap().metadata)
        else:
            print(i,"ERROR",result.unwrap_err())
    # T.sleep(100)
    

if __name__ == "__main__":
    example_run()
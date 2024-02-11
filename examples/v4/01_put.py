import os
import sys
from mictlanx.v4.client import Client
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils

def example_run():
    
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/01_put.py <BUCKET_ID> <PATH>")
    
    bucket_id  = Utils.get_or_default(args,0,default="mictlanx").unwrap()
    path       = Utils.get_or_default(args,1,default="./01_put.py").unwrap()

    peers =  Utils.peers_from_str_v2(peers_str=os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000") , protocol=os.environ.get("MICTLANX_PROTOCOL")) 
    # bucket_id = "public-bucket-0"

    
    client = Client(
        client_id    = os.environ.get("CLIENT_ID","client-0"),
        # 
        peers        = list(peers),
        # 
        debug        = True,
        # 
        daemon       = True, 
        # 
        max_workers  = 2,
        # 
        lb_algorithm ="2CHOICES_UF",
        bucket_id= bucket_id 
    )
    
    x = client.put_file_chunked(
        path=path,
        chunk_size="1MB",
        bucket_id=bucket_id,
        tags={"test":"TAG"}
    )
    print(x)

if __name__ == "__main__":
    example_run()
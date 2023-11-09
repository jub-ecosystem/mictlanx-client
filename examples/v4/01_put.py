import os
import sys
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import PutResponse
from option import Result
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils

def example_run():
    
    args = sys.argv[1:]
    if(len(args) >= 2  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/01_put.py <PATH>")
    
    path  = args[0]
    peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000")) 

    client = Client(
        client_id    = "client-example-0",
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
        bucket_id= "B1"
    )
    # 

    with open(path,"rb") as f:
        value:bytes                     = f.read()
        result                          = client.put(
            value     = value,
            tags      = {"example_name":"01_put"},
            # bucket_id = "MICTLANXCUSTOM"
        )
        # 
        # Mas codigo
        # 
        x:Result[PutResponse,Exception] = result.result()
        # 
        print(x)


if __name__ == "__main__":
    example_run()
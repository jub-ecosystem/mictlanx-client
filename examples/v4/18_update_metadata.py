
import os
import sys
from mictlanx.v4.client import Client
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils
import mictlanx.v4.interfaces as InterfaceX

def example_run():
    
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/18_update_metadata.py <BUCKET_ID> <KEY>")
    
    # bucket_id  = Utils.get_or_default(args,0,default="mictlanx").unwrap()
    # path       = Utils.get_or_default(args,1,default="./01_put.py").unwrap()
    
    bucket_id  = args[0]
    # Utils.get_or_default(args,0,default="mictlanx").unwrap()
    key       = args[1]
    # rf = int(args[2])
    # Utils.get_or_default(args,,default="./01_put.py").unwrap()

    peers =  Utils.routers_from_str(
        routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
        protocol=os.environ.get("MICTLANX_PROTOCOL","http")
    ) 
    # bucket_id = "public-bucket-0"

    
    client = Client(
        client_id    = os.environ.get("CLIENT_ID","client-0"),
        routers        = list(peers),
        debug        = True,
        max_workers  = 2,
        bucket_id= bucket_id,
        log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
    )
    

    # client.delete
    metadata = InterfaceX.Metadata(
        bucket_id="jcastillo",
        key="1f49004cac2e33afe6896ad8738055e65d3136f94b58f7299a6523f790898e8e",
        ball_id="1f49004cac2e33afe6896ad8738055e65d3136f94b58f7299a6523f790898e8e",
        checksum="6ef4f81590acab6db33fadf14358a5217b5d74069ec4987a9d65432ffdf43ecb",
        content_type="text/plain",
        is_disabled=False,
        producer_id="mictlanx-sync-0",
        size=4768,
        tags={
            "filename": "gamma_containers_2",
            "bucket_relative_path": "gamma_containers_2",
            "updated_at": "1727409880",
            "full_path": "/sink/mictlanx-sync/jcastillo/gamma_containers_2",
            "extension": "",
            "fullname": "gamma_containers_2"
        }
    )
    x = client.update_metadata(
        bucket_id= bucket_id,
        key = key,
        metadata = metadata 
    )
    print(x)

if __name__ == "__main__":
    example_run()
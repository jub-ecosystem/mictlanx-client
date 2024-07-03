import os
import sys
from mictlanx.v4.client import Client
from xolo.utils.utils import Utils as XoloUtils
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils
# from mictlanx.v4.interfaces.index import Peer

def example_run():
    
    args = sys.argv[1:]
    print(args)
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/16_put_encrypt.py <BUCKET_ID> <KEY>")
    
    # bucket_id  = Utils.get_or_default(args,0,default="mictlanx").unwrap()
    key       = Utils.get_or_default(args,1,default="").unwrap()
    
    bucket_id  = args[0]
    # Utils.get_or_default(args,0,default="mictlanx").unwrap()
    # Utils.get_or_default(args,,default="./01_put.py").unwrap()

    routers =  Utils.routers_from_str(
        routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
        protocol=os.environ.get("MICTLANX_PROTOCOL","http")
    ) 
    # bucket_id = "public-bucket-0"

    
    client = Client(
        client_id    = os.environ.get("CLIENT_ID","client-0"),
        routers        = list(routers),
        debug        = True,
        max_workers  = 2,
        bucket_id= bucket_id,
        log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
    )
    

    # Create a key pair (thi)
    # x = XoloUtils.X25519_key_pair_generator("bob")
    
    jcastillo_keypair = XoloUtils.load_private_key(filename="jcastillo").unwrap()
    bob_pub = XoloUtils.load_public_key(filename="bob").unwrap()
    shared_key = jcastillo_keypair.exchange(peer_public_key=bob_pub)

    x = client.put_encrypt(
        bucket_id=bucket_id,
        key=key,
        secret_key=shared_key,
        value=b"SECRET MESSAGE",
        chunk_size="1MB",
        tags={
            "example":"16_put_encrypt.py"
        }
    )
    print(x)

if __name__ == "__main__":
    example_run()
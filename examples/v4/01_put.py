import os
import sys
# Importar cliente
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
    bucket_id = "public-bucket-0"

    
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
        bucket_id= bucket_id 
    )
    
    x = client.put_file_chunked(path=path,chunk_size="1MB", bucket_id=bucket_id,tags={"test":"TAG"})
    print(x)

    # with open(path,"rb") as f:
    #     # Se leen los bytes de los archivos (pueden ser archivos cifrados)
    #     value:bytes                     = f.read()
    #     # Se utiliza cliente para relaizar una escritura (put)
    #     put_result                          = client.put(
    #         # key       = "RICHI",
    #         value     = value,
    #         tags      = {
    #             "example_name":"01_put",
    #             # "description":"Estos datos se generaron en una meet con ricardo."
    #         },
    #     )
    #     # 
    #     x:Result[PutResponse,Exception] = put_result.result()
    #     print(x)


if __name__ == "__main__":
    example_run()
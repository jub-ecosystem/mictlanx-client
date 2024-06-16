
import os
import sys
# Importar cliente
from mictlanx.v4.client import Client

from mictlanx.v4.interfaces.responses import PutResponse

from option import Result
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils
import humanfriendly as HF
import requests as R

def example_run():
    
    args = sys.argv[1:]
    if(len(args) >= 3  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v4/11_put_chunked.py <PATH> <CHUNK_SIZE>")
    
    path  = args[0]
    routers        =  list(Utils.routers_from_str(routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")))
    chunk_size = args[1]

    
    bucket_id = "mictlanx"
    client = Client(
        client_id    = "client-example-0",
        # 
        routers        =  routers,
        # 
        debug        = True,
        # 
        max_workers  = 2,
        # 
        lb_algorithm ="2CHOICES_UF",
        bucket_id= bucket_id
    )
    
    chunks = Utils.file_to_chunks_gen(path=path, chunk_size=chunk_size)

    res = client.put_chunked(
        chunks=chunks,
        bucket_id=bucket_id,
        key="",
        tags={
            "example":"11_put_chunked.py"
        }
    )
    print(res)
    # headers = {
    #     "Content-Type": "application/octet-stream"
    # }
    # task_id = "t_b0mFy3Q-F8RDGCAy6XVTw"
    # url = "http://localhost:7000/api/v4/buckets/data/{}/chunked".format(task_id)
    # try:
    #     response = R.post(url =url,data = chunks,headers=headers)
    #     response.raise_for_status()
    #     print("RESPONSE",response)
    # except Exception as e:
    #     print("Error {}".format(e))
        # Se utiliza cliente para relaizar una escritura (put)
        # put_result                          = client.put(
        #     # key       = "RICHI",
        #     value     = value,
        #     tags      = {
        #         "example_name":"01_put",
        #         # "description":"Estos datos se generaron en una meet con ricardo."
        #     },
        # )
        # 
        # x:Result[PutResponse,Exception] = put_result.result()
        # print(x)


if __name__ == "__main__":
    example_run()
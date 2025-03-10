import os
import sys
import time as T
import pandas as pd
from mictlanx import AsyncClient
import dotenv 
import asyncio
import humanfriendly as HF
dotenv.load_dotenv()

from mictlanx.utils.index import Utils

peers     = Utils.routers_from_str(
    routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
    protocol=os.environ.get("MICTLANX_PROTOCOL","https")
) 

client = AsyncClient(
    client_id    = os.environ.get("CLIENT_ID","client-0"),
    routers        = list(peers),
    debug        = True,
    max_workers  = 8,
    log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
)
TRACE_PATH     = os.environ.get("TRACE_PATH","/traces")
TRACE_FILENAME = os.environ.get("TRACE_FILENAME","mx-1000_10_unif_expo")
CHUNK_SIZE     = os.environ.get("CHUNK_SIZE","256kb")
SOURCE_PATH    = os.environ.get("SOURCE_PATH",f"/source/{TRACE_FILENAME}")
TIMEOUT        = int(os.environ.get("TIMEOUT",120))
MAX_TRIES      = int(os.environ.get("MAX_TRIES",10))
MAX_PARALLEL_GETS = int(os.environ.get("MAX_PARELLEL_GETS",10))
async def main():
    trace = pd.read_csv(f"{TRACE_PATH}/{TRACE_FILENAME}.csv")
    t1_global = T.time()
    for index,row in trace.iterrows():
        bucket_id = row["bucket_id"]
        key       = row["key"]
        iat       = row["interarrival_time"]
        operation = row["operation"]
        size      = row["size"]
        t1 = T.time()
        if operation == "PUT":
            path = f"{SOURCE_PATH}/{bucket_id}/{key}"
            res = await client.put_file(bucket_id=bucket_id, key=key, path=path, chunk_size=CHUNK_SIZE,timeout=TIMEOUT,max_tries=MAX_TRIES)
        else:
            res = await client.get(bucket_id=bucket_id,key=key,chunk_size=CHUNK_SIZE,timeout=TIMEOUT,max_paralell_gets=MAX_PARALLEL_GETS)
        rt = T.time() -t1
        print(f"{operation} {bucket_id} {key} {HF.format_size(size)} {res.is_ok} {HF.format_timespan(rt)}")
        T.sleep(iat)
    rt_global = T.time() - t1_global
    print("TOTAL_RESPONSE_TIME", rt_global)

if __name__ == "__main__":
    asyncio.run(main())
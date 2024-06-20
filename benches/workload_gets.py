import os
import sys
import time as T
import pandas as pd
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import PutResponse
from option import Result
from queue import Queue
from threading import Thread
from concurrent.futures import as_completed
import scipy.stats as S
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils

def consumer(q:Queue,c:Client):
    try:
        print("CONSUMER")
        pareto = S.pareto(1) 
        while True:
            key             = q.get()
            print("CONSUME",key)
            num_downloads   = pareto.rvs()
            num_downloads   = int(200 if num_downloads > 100 else num_downloads)
            futures = []
            for i in range(num_downloads):
                fut = c.get_async(key=key)
                futures.append(fut)
                
            for fut in as_completed(futures):
                print(fut.result())
                #print(i,key,get_response.result())
    except Exception as e:
        print(e)

def main():
    q     = Queue(maxsize=100)
    peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    c = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True, 
        max_workers = 4
    )
    try:

        trace           = pd.read_csv("/test/files_10MB/trace.csv")
        print(trace.shape)
        consumer_thread = Thread(target=consumer,kwargs={"q":q,"c":c},daemon=True)
        consumer_thread.start()
        for index,row in trace.iterrows():
            key  = row["FILE_ID"]
            print("KEY",key)
            q.put(key)
            T.sleep(1)
            print("_"*20)
        consumer_thread.join()
    except Exception as e:
        print(e)
if __name__ =="__main__":
    main()
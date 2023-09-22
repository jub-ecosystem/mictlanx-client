import os
import sys
import time as T
import pandas as pd
import numpy as np
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import PutResponse
from mictlanx.utils.segmentation import Chunks
from option import Result
from queue import Queue
from threading import Thread
from concurrent.futures import ThreadPoolExecutor,as_completed
import scipy.stats as S
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils





def consumer(q:Queue,c:Client):
    print("CONSUMER")
    try:
        pareto = S.pareto(1) 
        while True:
            key = q.get()
            num_downloads = pareto.rvs()
            num_downloads = int(100 if num_downloads > 100 else num_downloads)
            futures = []
            for i in range(num_downloads):
                get_response = c.get_and_merge_ndarray(key=key)
                futures.append(get_response)
            for fut in as_completed(futures):
                print(fut)
                # print("GET_RESPNSE[{}]".format(i),get_response)
    except Exception as e:
        print(e)


def put(index:int, q:Queue,c:Client,key:str):
    try:
        ndarray = np.random.random(size=(1000,100,10))
        chunks = Chunks.from_ndarray(ndarray=ndarray,group_id=key).unwrap()
        # print("CHUNKS",chunks)
        xs = c.put_chunks(key= key,chunks=chunks,tags={} )
        for  i,x in enumerate(xs):
            print("RESULT[{}]".format(i),x)
        print("_"*50)
        q.put(key)
    except Exception as e:
        print(e)
    # with open(path, "rb") as f:
    #     data = f.read()
    #     x    = c.put(value=data,tags={"bench":"01"},key=key ).result()
    #     print(index,x)
    #     q.put(key)
        # futures.append(x)

def producer(q:Queue,c:Client):
    try:
        # trace              = pd.read_csv("/home/nacho/Programming/Python/mictlanx/benches/traces/trace.csv")
        # for i in range()
        interarrival_times = S.expon(.1).rvs(size=100)
        futures            = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            for index,iat in enumerate(interarrival_times):
                key  = "matrix-{}".format(index)
                # print("PUT {}".format(key))
                executor.submit(put,index=index,q=q,c=c,key=key)
                T.sleep(iat)
            # T.sleep(100)
            # for index,row in trace.iterrows():
            #     path = row["PATH"]
            #     key  = row["FILE_ID"]
                # T.sleep(interarrival_times[index])
    except Exception as e:
        print(e)

    # for fut in futures:
        # print(fut.result())

        # num_downloads = pareto.rvs()
        # num_downloads = 100 if num_downloads > 100  else num_downloads
        # print("DOWNLOADS",event, num_downloads)


def main():
    q = Queue(maxsize=100)

    peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
    c = Client(
        client_id   = "client-example-0",
        peers       = list(peers),
        debug       = True,
        daemon      = True, 
        max_workers = 4
    )
    try:

        producer_thread = Thread(target=producer,kwargs={"q":q,"c":c},daemon=True)
        consumer_thread = Thread(target=consumer,kwargs={"q":q,"c":c},daemon=True)
        producer_thread.start()
        consumer_thread.start()
        
        producer_thread.join()
        consumer_thread.join()
        # T.sleep(100)
    except Exception as e:
        print(e)
if __name__ =="__main__":
    main()
import os
import sys
import time as T
import pandas as pd
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import PutResponse
from option import Result
from queue import Queue
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
import scipy.stats as S
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils





def consumer(q:Queue,c:Client):
    try:
        pareto = S.pareto(1) 
        while True:
            key             = q.get()
            num_downloads   = pareto.rvs()
            num_downloads   = int(200 if num_downloads > 100 else num_downloads)
            for i in range(num_downloads):
                get_response = c.get(key=key)
                print(i,key,get_response)
    except Exception as e:
        print(e)



def producer(q:Queue,c:Client):
    try:
        trace              = pd.read_csv("/test/files_10MB/trace.csv")
        print(trace.columns)
        # for i in range()
        interarrival_times = S.expon(1).rvs(size=trace.shape[0])
        futures            = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            for index,row in trace.iterrows():
                # path = row["PATH"]
                key  = row["FILE_ID"]
                executor.submit(consumer,index=index,q=q,c=c,key=key,path=path)
                T.sleep(interarrival_times[index])
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
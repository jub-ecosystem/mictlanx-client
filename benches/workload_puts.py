import os
import pandas as pd
import numpy as np
import sys
import time as T
import pandas as pd
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import PutResponse
from option import Result
from queue import Queue
from threading import Thread,Event
from concurrent.futures import ThreadPoolExecutor,as_completed
import scipy.stats as S
import dotenv 
dotenv.load_dotenv()
from mictlanx.utils.index import Utils
from mictlanx.logger.log import Log

log = Log(
    name="workload_puts-0",
    console_handler_filter=lambda x: True,
)

e = Event()

# peers =  Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","localhost:7000")) 
routers = list(Utils.routers_from_str(routers_str="router-0:localhost:60666"))
c = Client(
    client_id   = "benche_workload_puts-10MB_110",
    debug       = True,
    max_workers = 4,
    routers     = routers
)
BUCKET_ID = "benches_workload_puts-10MB_110"



def put(start_time:float,index:int, row:pd.Series):
    status = -1
    waiting_time = T.time() - start_time
    path = row["PATH"]
    try:
        st = np.random.randint(low=1, high=2)
        result = c.put_file_chunked(path=path,bucket_id=BUCKET_ID)
        if result.is_err:
            log.error({
                "msg":str(result.unwrap_err())
            })
        else:
            log.debug({
                "event":"PUT",
                "index":index,
                "path":path,
                "service_time":st,
                "waiting_time":waiting_time
            })
        # print(index,path,st,waiting_time)
        T.sleep(st)
        status=0
        # return 0,index, row
    except Exception as e:
        print(e)
        status = -1
    finally:
        return status, index, row


def run(q:Queue,n:int,max_workers:int =4):
    def _run():
        global_start_time = T.time()
        task_counter = 0
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers ) as tp:
            while True:
                if task_counter == n :
                    completed_ops = 0
                    
                    for fut in as_completed(futures):
                        status, index,row = fut.result()
                        if status ==0:
                            completed_ops+=1
                        # log.debug({
                        #     "event":"RESULT",
                        #     "path":row["PATH"],
                        #     "status":status
                        # })
                        # print(,row["FILE_ID"])
                    log.info({
                        "event":"STATS",
                        "operation_counter":n,
                        "completed_operation":completed_ops,
                        "failed_operations":n - completed_ops
                    })
                    global e 
                    e.set()
                try:
                    start_time = T.time()
                    i,row      = q.get(block=True)
                    fut = tp.submit(put, start_time=start_time,index=i, row=row)
                    futures.append(fut)
                except Exception as e :
                    print(e)
                finally:
                    task_counter+=1
    return _run

def main():
    start_time = T.time()
    q = Queue(maxsize=100)
    trace              = pd.read_csv("/test/files_10MB/trace.csv")
    max_workers = 4
    t = Thread(target=run(q,n=trace.shape[0],max_workers=max_workers),daemon=True)
    t.start()
    for i,row in trace.iterrows():
        q.put((i,row))
        T.sleep(1)
    
    e.wait()
    log.info({
        "event":"BENCH.COMPLETED",
        "time":T.time()-start_time
    })

    
        # T.sleep(2)
        # result = c.put_file_chunked(path=row["PATH"],bucket_id="test",tags={"experiment_id":"files_10MB_producer"})


    # try:

    #     producer_thread = Thread(target=producer,kwargs={"q":q,"c":c},daemon=True)
    #     # consumer_thread = Thread(target=consumer,kwargs={"q":q,"c":c},daemon=True)
    #     producer_thread.start()
    #     # consumer_thread.start()
        
    #     producer_thread.join()
    #     # consumer_thread.join()
    #     # T.sleep(100)
    # except Exception as e:
    #     print(e)
if __name__ =="__main__":
    main()
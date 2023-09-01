import pandas as pd
import os
import time as T
from concurrent.futures import ThreadPoolExecutor,wait,ALL_COMPLETED
from mictlanx.v3.interfaces.payloads import PutPayload
from mictlanx.v3.client import Client
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.services.replica_manger import ReplicaManager
from dotenv import load_dotenv
from option import Some,NONE
import pandas as pd
import queue
import threading
from concurrent.futures import wait,ALL_COMPLETED
import scipy.stats as S
load_dotenv()

replica_manager  = ReplicaManager(ip_addr = os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"), port=int(os.environ.get("MICTLANX_REPLICA_MANAGER_PORT",20000)), api_version=Some(3))
xolo             = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port=int(os.environ.get("MICTLANX_XOLO_PORT",10000)), api_version=Some(3))
proxy            = Proxy(ip_addr = os.environ.get("MICTLANX_PROXY_IP_ADDR"), port=int(os.environ.get("MICTLANX_PROXY_PORT",8080)), api_version=Some(3))
secret           = os.environ.get("MICTLANX_SECRET")
expires_in       = os.environ.get("MICTLANX_EXPIRES_IN","1d")
app_id           = os.environ.get("MICTLANX_APP_ID")
client_id        = os.environ.get("MICTLANX_CLIENT_ID")

c             = Client(
    app_id          = app_id,
    client_id       = Some(client_id),
    replica_manager = replica_manager,
    xolo            = xolo,
    proxies         = [proxy],
    secret          = secret,
    expires_in      = Some(expires_in),
    log_name="mictlanx-client_0"
)
MAX_DOWNLOADS = os.environ.get("MAX_DOWNLOADS",200)


def download(key):
    thread_id = threading.current_thread().name
    print("[{}] Download {}".format(thread_id,key))
    res = c.get(key=key)
    return res

def consumer(q:queue.Queue):
    dist = S.pareto(.5, loc=.5, scale=1)
    while True:
        try:

            key             = q.get(timeout=100)
            downloads_count = 1
            # int(dist.rvs())
            # downloads_count = MAX_DOWNLOADS if downloads_count >= MAX_DOWNLOADS else downloads_count
            # downloads_count = 
            # MAX_DOWNLOADS if downloads_count >= MAX_DOWNLOADS else downloads_count
            print("Consumed", key,"Downloads", downloads_count)
            # tasks = []
            with ThreadPoolExecutor(max_workers=1,thread_name_prefix="mictlanx") as executor:
                for i in range(downloads_count):
                    task = executor.submit(download,key)
                    # tasks.append(task)
                # completed_tasks,_ = wait(tasks,return_when=ALL_COMPLETED)
                # # for completed_task in completed_tasks:
                #     print(completed_task.result())

        except Exception as e:
            break

def write(*args):
    q,file_id , path = args
    with open(path,"rb") as f:
        data     = f.read()
        metadata = {}
        res      = c.put(
            key="{}".format(file_id),
            value = data,
            group_id=file_id,
            update=not False
        )
        if res.is_ok:
            q.put(file_id)
    T.sleep(1)

    return res

def run():
    q = queue.Queue()
    consumer_thread = threading.Thread(target=consumer,args=(q,))
    consumer_thread.setDaemon(True)
    consumer_thread.start()
    max_workers = 1
    try:
        # trace_path ="/home/nacho/Programming/Python/mictlanx/benches/traces/trace.csv"
        trace_path ="/test/files_10MB/trace.csv"
        df = pd.read_csv(trace_path)
        tasks = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            file_ids = []
            for index,row in list(df.iterrows())[:]:
                path    = row["PATH"]
                file_id = row["FILE_ID"]
                file_ids.append(file_id)
                fut = executor.submit(write, q,file_id,path)
                tasks.append(fut)
            completed_tasks,_ = wait(tasks,return_when=ALL_COMPLETED)
            for completed_task in completed_tasks:
                print(completed_task.result())
            print("BEFORE DELETE")
            T.sleep(60)
            for file_id in file_ids:
                c.delete(key=file_id)
            
    except Exception as e:
        print(e)
    finally:
        consumer_thread.join()
        c.logout()
        
                # print(res)

if __name__ == "__main__":
    run()
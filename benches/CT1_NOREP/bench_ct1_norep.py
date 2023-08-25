import sys
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
load_dotenv()

replica_manager  = ReplicaManager(ip_addr = os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"), port=int(os.environ.get("MICTLANX_REPLICA_MANAGER_PORT",20000)), api_version=Some(3))
xolo             = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port=int(os.environ.get("MICTLANX_XOLO_PORT",10000)), api_version=Some(3))
proxy            = Proxy(ip_addr = os.environ.get("MICTLANX_PROXY_IP_ADDR"), port=int(os.environ.get("MICTLANX_PROXY_PORT",8080)), api_version=Some(3))
secret           = os.environ.get("MICTLANX_SECRET")
expires_in       = os.environ.get("MICTLANX_EXPIRES_IN","1d")
app_id           = os.environ.get("MICTLANX_APP_ID")
client_id        = os.environ.get("MICTLANX_CLIENT_ID")




def ct1_norep_1mb():
    """CT1_NOREP_1MB"""
    c             = Client(
        app_id          = app_id,
        client_id       = Some(client_id),
        replica_manager = replica_manager,
        xolo            = xolo,
        proxies         = [proxy],
        secret          = secret,
        expires_in      = Some(expires_in)
    )
    path = "/source/01.pdf"
    for i in range(100):
        with open(path,"rb") as f:
            data     = f.read()
            metadata = {}
            res      = c.put(
                key="ct1MB_{}".format(i),
                value = data,
                group_id="ball_0",
                update=True
            )
        print(res)
        # T.sleep(1)
    c.logout()



def ct1_norep_10mb():
    """CT1_NOREP_10MB"""
    c             = Client(
        app_id          = app_id,
        client_id       = Some(client_id),
        replica_manager = replica_manager,
        xolo            = xolo,
        proxies         = [proxy],
        secret          = secret,
        expires_in      = Some(expires_in)
    )
    path = "/source/traces.tar"
    for i in range(100):
        with open(path,"rb") as f:
            data     = f.read()
            metadata = {}
            res      = c.put(
                key="ct10mb_{}".format(i),
                value = data,
                group_id="ball_1",
                update=True
            )
        print(res)
        # T.sleep(1)
    c.logout()


def _read(c:Client,ball_id:str):
    print("Read {}".format(ball_id))
    res = c.get(key=ball_id,cache=False, force=True)
    return res


def ct1_1mb_reads():
    """CT1_NOREP_1MB"""
    c             = Client(
        app_id          = app_id,
        client_id       = Some(client_id),
        replica_manager = replica_manager,
        xolo            = xolo,
        proxies         = [proxy],
        secret          = secret,
        expires_in      = Some(expires_in)
    )
        # T.sleep(1)
        # return ball_id

    try:
        tasks = []
        MAX_ITER = 100
        with ThreadPoolExecutor(max_workers=1) as ex:
            for i in range(0,MAX_ITER):
                ball_id = "ct1MB_{}".format(i)
                fut = ex.submit(_read,c,ball_id)
                tasks.append(fut)
            completed_tasks,_ = wait(tasks, return_when=ALL_COMPLETED)
            for completed_task in completed_tasks:
                print(completed_task.result())
    except Exception as e: 
        print("Error {}".format(e))
    finally:
        c.logout()

def ct1_10mb_reads():
    """CT1_NOREP_1MB"""
    c             = Client(
        app_id          = app_id,
        client_id       = Some(client_id),
        replica_manager = replica_manager,
        xolo            = xolo,
        proxies         = [proxy],
        secret          = secret,
        expires_in      = Some(expires_in)
    )
    try:
        tasks = []
        MAX_ITER=100
        with ThreadPoolExecutor(max_workers=1) as ex:
            for i in range(0,MAX_ITER):
                ball_id = "ct10mb_{}".format(i)
                fut = ex.submit(_read,c,ball_id)
                tasks.append(fut)
            completed_tasks,_ = wait(tasks, return_when=ALL_COMPLETED)
            for completed_task in completed_tasks:
                print(completed_task.result())
    except Exception as e: 
        print("Error {}".format(e))
    finally:
        c.logout()


def producer_consumer():
    c             = Client(
        app_id          = app_id,
        client_id       = Some(client_id),
        replica_manager = replica_manager,
        xolo            = xolo,
        proxies         = [proxy],
        secret          = secret,
        expires_in      = Some(expires_in)
    )
    try:
        df = pd.read_csv("/home/nacho/Programming/Python/mictlanx/benches/CT1_NOREP/traces/1mb.csv")
        for index,row in df.iterrows():
            path    = row["PATH"]
            file_id = row["FILE_ID"]
            with open(path,"rb") as f:
                data     = f.read()
                metadata = {}
                res      = c.put(
                    key="{}".format(file_id),
                    value = data,
                    group_id="ball_1",
                    update=True
                )
                print(res)
    except Exception as e: 
        print("Error {}".format(e))
    finally:
        c.logout()


__benchmarks__= [
    (producer_consumer,producer_consumer, "Producer consumer")
    # (ct1_norep_1mb,ct1_norep_10mb,"No replication (RF=1) - Concurrent traffic (writes) = 1"),
    # (ct1_1mb_reads,ct1_10mb_reads, "No replication (RF=1) - Concurrent traffic (reads) = 1"),

]
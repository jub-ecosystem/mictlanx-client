
import os
import json as J
import time as T
import requests as R
import platform
PLATFORM_ID = platform.platform().lower()
import humanfriendly as HF
from queue import Queue
from option import Result,Ok,Err,Option,NONE,Some
from typing import List,Dict,Tuple
from mictlanx.logger.log import Log
from threading import Thread,Lock
from concurrent.futures import ThreadPoolExecutor,as_completed
from mictlanx.v4.interfaces.index import Peer,PeerStats
from mictlanx.v4.xolo.utils import Utils as XoloUtils
import logging as L
from uuid import uuid4
from mictlanx.utils.index import Utils

from abc import ABC

API_VERSION = 4 

class Task(ABC):
    def __init__(self,task_type:str,retries:int =0):
        self.task_id = str(uuid4().hex)
        self.task_type = task_type
        self.arrival_time = T.time()
        self.retries = retries 

    # def __str__(self):
    #     return "Task({})".format(self.id)

class PutTask(Task):
    def __init__(self,bucket_id:str,key:str,checksum:str,size:int,path:str,chunk_size:str ="1MB",producer_id:str="mictlanx",task_type: str="PUT"):
        super().__init__(task_type)
        self.bucket_id = bucket_id
        self.key = key
        self.checksum =checksum 
        self.size=size
        self.path:str=path
        self.chunk_size = chunk_size
        self.producer_id = producer_id
    def __str__(self):
        return "PutTask({},{})".format(self.arrival_time,self.task_id)

class GetTask(Task):
    def __init__(self,bucket_id:str, key:str,chunk_size:str="1MB", task_type: str="GET"):
        super().__init__(task_type)
        self.bucket_id = bucket_id
        self.key = key
        self.chunk_size = chunk_size
    def __str__(self) -> str:
        return "GetTask({},{})".format(self.arrival_time,self.task_id)

class AssignPeer(Task):
    def __init__(self, peer_id:str,event_id:str,task_type: str="ASSIGN_PEER"):
        super().__init__(task_type)
        self.event_id = event_id
        self.peer_id = peer_id

class Enqueue(Task):
    def __init__(self,event_id:str,n:int =0, task_type: str="ENQUEUE"):
        super().__init__(task_type)
        self.n = n
        self.event_id = event_id

class Balancing(Task):
    def __init__(self,bucket_id:str,key:str,size:int,event_id:str,algorithm:str="ROUND_ROBIN",operation_type:str="PUT",task_type:str="BALANCNG"):
        super().__init__(task_type)
        # self.ball = LoadBalancingBall(key, size)
        self.bucket_id = bucket_id
        self.key = key
        self.size = size
        self.task_type = task_type
        self.operation_type = operation_type
        self.algorithm = algorithm
        self.event_id = event_id
        _combined_key  = "{}@{}".format(bucket_id,key)
        self.combined_key = XoloUtils.sha256(_combined_key.encode("utf-8"))
        # self.task
    def __str__(self):
        return "Task({})".format(self.task_id)
    

class UnavilablePeer(Task):
    def __init__(self,peer_id:str, task_type: str = "UNAVAILABLE_PEER"):
        super().__init__(task_type)
        self.peer_id = peer_id
# ___________________________________

class Event(ABC):
    def __init__(self,event_type:str,task_id:str,arrival_time:float, started_at:float, departure_at:float, waiting_time:int = 0,retries:int=0):
        self.event_id  = uuid4().hex
        self.task_id   = task_id
        self.event_type = event_type 
        self.arrival_time = arrival_time
        self.started_at = started_at
        self.departure_at = departure_at
        self.waiting_time = waiting_time
        self.service_time = -1
        self.idle_time = -1
        self.created_at = T.time()
        self.retries = retries
        self.response_time = -1
        # self.response_time = self.waiting_time + self.service_time
    def update_service_and_response_time(self):
        self.service_time = self.departure_at - self.started_at
        self.response_time = self.waiting_time + self.service_time
        return self.service_time
        # self.completed =
    def update_waiting_time(self,ld:float):
        if self.arrival_time < ld:
            self.waiting_time = ld - self.arrival_time
            self.idle_time=0
        else:
            self.waiting_time=0
            self.idle_time  = self.arrival_time - ld

class Put(Event):
    def __init__(self,arrival_time:float,started_at:float,departure_at:float,task_id:str,bucket_id:str,key:str,checksum:str,size:int,path:str,peer_id:str,chunk_size:str ="1MB",producer_id:str="mictlanx",task_type: str="PUT",waiting_time:int=0,retries:int=0):
        super().__init__(event_type=task_type,task_id=task_id, arrival_time=arrival_time,started_at=started_at,departure_at=departure_at,waiting_time=waiting_time,retries=retries)
        self.bucket_id  = bucket_id
        self.key        = key
        self.checksum   = checksum 
        self.size       = size
        self.path       = path
        self.chunk_size = HF.parse_size(chunk_size)
        self.producer_id = producer_id
        self.peer_id = peer_id
class Get(Event):
    def __init__(self,bucket_id:str,key:str,peer_id:str, task_id: str, arrival_time: float, started_at: float, departure_at: float, waiting_time: int = 0,chunk_size:str="1MB",event_type: str="GET",sink_folder_path:str = "/mictlanx/data"):
        super().__init__(event_type, task_id, arrival_time, started_at, departure_at, waiting_time)
        self.bucket_id = bucket_id
        self.key = key
        self.peer_id = peer_id
        self.chunk_size = chunk_size
        self.sink_folder_path = sink_folder_path
        
class BalancingSuccessed(Event):
    def __init__(self,peer_id:str, task_id: str, arrival_time: float, started_at: float, departure_at: float, event_type: str="BALANCING_SUCCEED"):
        super().__init__(event_type, task_id, arrival_time, started_at, departure_at)
        self.peer_id = peer_id
    # def combined_key
# _____________________________________________________

class AsyncClientHandler(Thread):
    def __init__(self,pending_task_q:Queue,lb_q:Queue,completed_tasks_q:Queue,peer_healer:"PeerHealer",heartbeat:str="5sec",name: str="tezcanalyticx", daemon:bool = True) -> None:
        Thread.__init__(self,name=name,daemon=daemon)
        self.is_running = True
        self.tasks:Dict[str,Task] = {}
        self.completed_tasks:List[str] = []
        self.pending_events:Dict[str, Event] ={}
        self.heartbeat = HF.parse_timespan(heartbeat)
        self.pening_task_q = pending_task_q
        self.lb_q = lb_q
        self.peer_healer:PeerHealer = peer_healer 
        self.completed_tasks_q =  completed_tasks_q
        self.last_arrival_time = 0
        self.last_departure_time = 0
        self.max_retries = 10
        self.__log =           Log(
            name = "mictlanx-async-handler",
            console_handler_filter=lambda x: True,
            interval=24,
            when="h"
        )

    def run(self) -> None:
        while self.is_running:
            try:
                task:Task = self.pening_task_q.get(block=True)
                self.tasks[task.task_id] = task
                # self.__log.debug({
                #     "arrival_time":task.arrival_time,
                #     "task_id":task.task_id,
                #     "task_type":task.task_type,
                #     "retries":task.retries
                # })
                
                if isinstance(task, PutTask):
                    put_task:PutTask = task
                    event:Put = Put(
                        arrival_time=task.arrival_time,
                        started_at=T.time(),
                        departure_at=0,
                        task_id=put_task.task_id,
                        bucket_id= put_task.bucket_id,
                        key= put_task.key,
                        checksum=put_task.checksum,
                        size=put_task.size,
                        path= put_task.path,
                        chunk_size=put_task.chunk_size,
                        peer_id="",
                        waiting_time=0,
                        
                    )
                    self.pending_events[event.event_id] = event
                    _e = Balancing(bucket_id=put_task.bucket_id,key= put_task.key, size= put_task.size,event_id=event.event_id)
                    self.lb_q.put(_e)
                elif isinstance(task, AssignPeer):
                    assign_peer:AssignPeer = task
                    event = self.pending_events[assign_peer.event_id]
                    


                    if event.event_type =="PUT":
                        put:Put = event
                        put.peer_id = assign_peer.peer_id
                        maybe_peer = self.peer_healer.get_peer(peer_id= put.peer_id)
                        if maybe_peer.is_none:
                            self.__log.error({
                                "msg":"Peer not found",
                                "peer_id":put.peer_id,
                            })
                        else:
                            peer = maybe_peer.unwrap()
                            self.pending_events[assign_peer.event_id] = put
                            
                            # self.__log.debug({
                            #     "event_id":put.event_id,
                            #     "task_id":assign_peer.task_id,
                            #     "task_type":assign_peer.task_type,
                            #     "peer_id":put.peer_id
                            # })
                            _start_time = T.time()
                            put_metadata_result = peer.put_metadata(
                                key          = put.key, 
                                size         = put.size, 
                                checksum     = put.checksum,
                                tags         = {},
                                producer_id  = put.producer_id,
                                content_type = "application/octet-stream",
                                ball_id      = put.key,
                                bucket_id    = put.bucket_id,
                                timeout      = 300,
                                is_disable   = False,
                            )
                            if put_metadata_result.is_err:
                                raise put_metadata_result.unwrap_err()
                            put_metadata_response_time = T.time() - _start_time
                            put_metadata_response = put_metadata_result.unwrap()
                            self.__log.info({
                                "event":"PUT.METADATA",
                                "event_id":put.event_id,
                                "task_id":assign_peer.task_id,
                                "task_type":assign_peer.task_type,
                                "peer_id":put.peer_id,
                                "service_time": put_metadata_response.service_time,
                            })
                            _start_time = T.time()
                            file_chunks = Utils.file_to_chunks_gen(path=put.path, chunk_size=HF.format_size(put.chunk_size));
                            x = peer.put_chuncked(
                                task_id= put_metadata_response.task_id,
                                chunks=file_chunks,
                            )
                            if x.is_err:
                                print("ROLLBACK",str(x.unwrap_err()))
                            else:
                                # wait = np.random.randint(low=5,high=10)
                                # print("WAIT",wait)
                                # T.sleep(wait)
                                put.departure_at = T.time()
                                # put.waiting_time = 0 if self.last_arrival_time ==0 else self.last_arrival_time
                                put.update_waiting_time(ld = self.last_departure_time)
                                # if put.arrival_time < self.last_departure_time:
                                #     put.waiting_time = self.last_departure_time - put.arrival_time
                                # else:
                                #     put.waiting_time=0
                                put.update_service_and_response_time()
                                self.last_arrival_time = put.arrival_time
                                self.last_departure_time=  put.departure_at
                                _task = self.tasks.pop(put.task_id)
                                self.completed_tasks_q.put(put)
                                self.__log.info({
                                    "event":"PUT.CHUNKED",
                                    "task_id":assign_peer.task_id,
                                    "task_type":assign_peer.task_type,
                                    "event_id":put.event_id,
                                    "arrival_time":put.arrival_time,
                                    "started_at":put.started_at,
                                    "departure_at":put.departure_at,
                                    "service_time":put.service_time,
                                    "waiting_time":put.waiting_time,
                                    "response_time":put.response_time,
                                    "bucket_id":put.event_id,
                                    "key":put.key,
                                    "size":put.size,
                                    "peer_id":put.peer_id,
                                    "put_chunked_response_time":T.time() -_start_time,
                                    "put_metadata_service_time": put_metadata_response.service_time,
                                    "put_metadata_response_time": put_metadata_response_time,

                                })

                    elif event.event_type =="GET":
                        get_event:Get = event
                        get_event.peer_id = assign_peer.peer_id
                        maybe_peer = self.peer_healer.get_peer(peer_id= get_event.peer_id)
                        # self.__log.debug({
                        #     "event":"ASSIGN.PEER.GET",
                        #     "bucket_id":get_event.bucket_id,
                        #     "key":get_event.key,
                        #     "peer_id":get_event.peer_id,
                        # })
                        if maybe_peer.is_none:
                            self.__log.error({
                                "msg":"Peer not found",
                                "peer_id":put.peer_id,
                            })
                            # print("NOT_FOUND")
                        else:
                            peer = maybe_peer.unwrap()
                            metadata_result = peer.get_metadata(bucket_id=get_event.bucket_id, key=get_event.key)

                            if metadata_result.is_ok:
                                metadata_response = metadata_result.unwrap()
                                self.__log.info({
                                    "event":"GET.METADATA",
                                    "event_id":get_event.event_id,
                                    "task_id":get_event.task_id,
                                    "bucket_id":get_event.bucket_id,
                                    "key":get_event.key,
                                    "service_time":metadata_response.service_time,
                                    "peer_id":metadata_response.node_id
                                })
                                
                                _combined_key ="{}@{}".format(get_event.bucket_id,get_event.key)
                                combined_key = XoloUtils.sha256(_combined_key.encode("utf-8"))
                                local_path = "{}/{}".format(get_event.sink_folder_path,combined_key)
                                if not os.path.exists(local_path):
                                    get_result = peer.get_to_file(
                                        bucket_id=get_event.bucket_id,
                                        key=get_event.key,
                                        chunk_size=get_event.chunk_size,
                                        sink_folder_path=get_event.sink_folder_path,
                                        filename = combined_key
                                    )

                                    if get_result.is_ok:
                                        path = get_result.unwrap()
                                        get_event.departure_at= T.time()
                                        get_event.update_waiting_time(ld = self.last_departure_time)
                                        get_event.update_service_and_response_time()
                                        self.__log.info({
                                            "event":"GET.TO.FILE",
                                            "event_id":get_event.event_id,
                                            "event_type":get_event.event_type,
                                            "arrival_time":get_event.arrival_time,
                                            "started_at":get_event.started_at,
                                            "departure_time":get_event.departure_at,
                                            "idle_time":get_event.idle_time,
                                            "waiting_time":get_event.waiting_time,
                                            "service_time":get_event.service_time,
                                            "response_time":get_event.response_time,
                                            "metadata_service_time":metadata_response.service_time,
                                            "path":path,
                                            "bucket_id":get_event.bucket_id,
                                            "key":get_event.key,
                                            "size":metadata_response.metadata.size,
                                            "peer_id":metadata_response.node_id,
                                            "retries":get_event.retries,
                                        })
                                        self.completed_tasks_q.put(get_event)
                                    else:
                                        self.__log.error({
                                            "msg":str(get_result.unwrap_err())
                                        })
                                else:
                                    (local_checksum,local_size) = XoloUtils.sha256_file(path=local_path )
                                    if local_checksum == metadata_response.metadata.checksum:
                                        self.__log.info({
                                            "event":"HIT",
                                            "bucket_id":get_event.bucket_id,
                                            "key":get_event.key,
                                            "peer_id":get_event.peer_id
                                        })
                                        self.completed_tasks_q.put(get_event)
                                    else:
                                        self.__log.error({
                                            "event":"CHECKSUM.NOT.MATCH",
                                            "local_checksum":local_checksum,
                                            "local_size":local_size,
                                            "remote_checksum":metadata_response.metadata.checksum,
                                            "peer_id":metadata_response.node_id,
                                        })
                            else:
                                self.pening_task_q.put(Enqueue(event_id=get_event.event_id,n= get_event.retries))
                                self.__log.error({
                                    "msg":str(metadata_result.unwrap_err())
                                })

                            # pe
                    else:
                        self.__log.error({
                            "msg":"Undefined event_type", 
                            "event_type":event.event_type
                        })
                elif isinstance(task,GetTask):
                    get_Task:GetTask = task
                    event:Get = Get(
                        task_id=get_Task.task_id,
                        bucket_id=get_Task.bucket_id,
                        key= get_Task.key,
                        peer_id="",
                        arrival_time=get_Task.arrival_time,
                        chunk_size=get_Task.chunk_size,
                        departure_at= 0,
                        started_at=T.time(),
                        waiting_time=0
                    )
                    self.pending_events[event.event_id] = event
                    self.lb_q.put(Balancing(bucket_id=get_Task.bucket_id,key= get_Task.key, size= 0,event_id=event.event_id,operation_type="GET"))
                elif isinstance(task, Enqueue):
                    if task.event_id in self.pending_events:
                        event = self.pending_events[task.event_id]
                        _task = self.tasks.get(event.task_id,None)
                        # print("EVENT_RETRIES",event.retries, "MAX_RETRIES",self.max_retries)
                        if event.retries <= self.max_retries and (not _task is None):
                            event.retries+= 1
                            _task.retries+=1
                            self.pending_events[task.event_id] = event
                            self.pening_task_q.put(_task)
                    else:
                        self.__log.error({
                            "msg":"EVENT.NOT.FOUND",
                            "task_id":task.task_id,
                            "event_id":task.event_id,
                            "task_type":task.task_type
                        })
                    # event = self.pending_events[task.event_id]
                else:
                    print("UKNOWN",task)
            except Exception as e:
                self.__log.error({
                    "msg":str(e)
                })

class PeerHealer(Thread):
    def __init__(self,q:Queue,peers:List[Peer],heartbeat:str="5sec",name: str="tezcanalyticx", daemon:bool = True, show_logs:bool=True) -> None:
        Thread.__init__(self,name=name,daemon=daemon)
        self.is_running = True
        self.heartbeat = HF.parse_timespan(heartbeat)
        # self.lock = Lock()
        self.operations_counter = 0
        self.peers=peers
        self.unavailable_peers = []
        # self.available_peers = []
        self.__peer_stats:Dict[str, PeerStats] = {}
        self.q= q
        # self.peers:List[LoadBalancingBin] = list(map(lambda x: , peers))
        self.tasks:List[Task] = []
        self.completed_tasks:List[str] = []
        self.__log             = Log(
            name = "tezcanalyticx_peer_healer",
            console_handler_filter=lambda x: show_logs,
            interval=24,
            when="h"
        )

    def get_peer(self,peer_id:str)->Option[Peer]:
        if not peer_id in self.unavailable_peers:
            maybe_peer = next( (  peer for peer in self.peers if peer.peer_id == peer_id ), None)
            if maybe_peer is None:
                return NONE
            else:
                return Some(maybe_peer)

    def get_stats(self):
        return self.__peer_stats
    # def get(task_id:str)->Result[]
    def run(self) -> None:
        while True:
            try:
                peers= self.peers
                counter = 0
                # unavailable_peers =[]
                self.unavailable_peers = []
                for peer in peers:
                    get_ufs_response = peer.get_ufs()
                    if get_ufs_response.is_ok:
                        response = get_ufs_response.unwrap()
                        peer_stats = self.__peer_stats.get(peer.peer_id,PeerStats(peer_id=peer.peer_id))

                        peer_stats.total_disk = response.total_disk
                        peer_stats.used_disk  = response.used_disk
                        self.__peer_stats[peer.peer_id] = peer_stats
                        counter +=1
                        self.__log.debug("Peer {} is  available".format(peer.peer_id))
                    else:
                        # self.peers
                        self.unavailable_peers.append(peer.peer_id)
                        self.__log.error("Peer {} is not available.".format(peer.peer_id))
                        
                        
                percentage_available_peers =  (counter / len(peers))*100 
                if percentage_available_peers == 0:
                    self.__log.error("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
                    for peer_id in self.unavailable_peers:
                        self.q.put(UnavilablePeer(peer_id=peer_id))
                    raise Exception("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
                elif percentage_available_peers < 100:
                    for peer_id in self.unavailable_peers:
                        self.q.put(UnavilablePeer(peer_id=peer_id))
                self.__log.debug("{}% of the peers are available".format(percentage_available_peers ))
                T.sleep(self.heartbeat)
            except Exception as e:
                print(e)




class LoadBalancingDaemon(Thread):
    def __init__(self,pending_task_q:Queue,balancing_q:Queue,peer_healer:PeerHealer,heartbeat:str="5sec",name: str="tezcanalyticx", daemon:bool = True) -> None:
        Thread.__init__(self,name=name,daemon=daemon)
        self.is_running = True
        self.heartbeat = HF.parse_timespan(heartbeat)
        self.pending_task_q = pending_task_q
        self.balancing_q = balancing_q
        self.lock = Lock()
        self.operations_counter = 0
        # self.peers:List[LoadBalancingBin] = list(map(lambda x: , peers))
        self.tasks:List[Task] = []
        self.completed_tasks:List[str] = []
        self.distribution_schema:Dict[str, List[str]] = {}
        self.peer_healer = peer_healer
        self.__log = Log(
            name = "tezcanalyticx_lb",
            console_handler_filter=lambda x: True,
            interval=24,
            when="h"
        )

    # def get(task_id:str)->Result[]
    
    def __round_robin(self,peers:List[str]=[])->Option[str]:
        n =len(peers)
        if n  == 0:
            return None
        if n == 1:
            return Some(peers[0])
        else:
            selected_bin_index = self.operations_counter % len(peers)
            return Some(peers[selected_bin_index])



    def __lb(self, peers:List[str]=[],algorithm:str="ROUND_ROBIN")->Option[str]:
        if algorithm == "ROUND_ROBIN":
            return self.__round_robin(peers=peers)
        else:
            return self.__round_robin(peers=peers)
        

    def run(self) -> None:
        while self.is_running:
            try:
                task:Task = self.balancing_q.get(block=True)
                # self.__log.debug({
                #     "arrival_time":task.arrival_time,
                #     "task_id":task.task_id,
                #     "task_type":task.task_type,
                # })
                if isinstance(task,Balancing):
                    balancing:Balancing = task
                    with self.lock:
                        self.tasks.append(task)
                        x = self.peer_healer.get_stats()

                        all_peers_ids = list(map(lambda x: x.peer_id,self.peer_healer.peers))

                        if balancing.operation_type == "PUT":
                            maybe_selected_peer_id = self.__lb(peers=all_peers_ids, algorithm= balancing.algorithm)
                            if maybe_selected_peer_id.is_none:
                                continue
                            selected_peer_id = maybe_selected_peer_id.unwrap()
                            if balancing.combined_key in self.distribution_schema:
                                self.distribution_schema[balancing.combined_key].append(selected_peer_id)
                            else:
                                self.distribution_schema[balancing.combined_key]=[selected_peer_id]


                            self.pending_task_q.put(AssignPeer(event_id=balancing.event_id,peer_id=selected_peer_id))
                            self.__log.debug({
                                "event":"BALANCING",
                                "algorithm":balancing.algorithm,
                                "selected_peer":selected_peer_id,
                                "operation_counter": self.operations_counter,
                                # "available_peers":all_peers_ids
                            })

                        elif balancing.operation_type=="GET":
                            peers_ids =self.distribution_schema[balancing.combined_key] if balancing.combined_key in self.distribution_schema else all_peers_ids
                            maybe_selected_peer_id = self.__lb(peers=peers_ids, algorithm= balancing.algorithm)
                            if maybe_selected_peer_id.is_none:
                                continue
                            selected_peer_id = maybe_selected_peer_id.unwrap()
                            # selected_peer_id = selected_peer.peer_id
                            # selected_bin_index = self.operations_counter % len(peers_ids)
                            # selected_peer_id = peers_ids[selected_bin_index]
                            self.pending_task_q.put(AssignPeer(event_id=balancing.event_id,peer_id=selected_peer_id))
                            self.__log.debug({
                                "event":"BALANCING",
                                "algorithm":balancing.algorithm,
                                "selected_peer":selected_peer_id,
                                "operation_counter": self.operations_counter,
                                "available_peers":peers_ids
                            })
                            
                        else:
                            self.__log.error({
                                "msg":"UKNOWN_OPERATION_TYPE",
                                "operation_type":balancing.operation_type
                            })
                        self.operations_counter +=1

                elif isinstance(task, UnavilablePeer):
                    _task:UnavilablePeer =  task
                    self.__log.debug({
                        "arrival_time":task.arrival_time,
                        "task_id":task.task_id,
                        "task_type":task.task_type,
                        "event":"UNAVAILABLE_PEER",
                        "peer_id":_task.peer_id
                    })
                else:
                    pass
                T.sleep(self.heartbeat)
            except Exception as e:
                print(e)
    



class AsyncClient(Thread):
    def __init__(
            self,
            client_id:str="async-client-0",
            peers:List[Peer] = [],
            debug:bool=True,
            # show_metrics:bool=True,
            # daemon:bool=True,
            max_workers:int = 12,
            lb_algorithm:str="ROUND_ROBIN",
            output_path:str = "/mictlanx/client",
            heartbeat_interval:str  = "5s",
            # metrics_buffer_size:int =  100,
            check_peers_availavility_interval:str = "10s",
            disable_log:bool = False,
            log_when:str="m",
            log_interval:int = 30,
            q_size:int = 100
    ):

        Thread.__init__(self,name=client_id,daemon=True)
        # Client unique identifier
        self.client_id = client_id
        # Heartbeats are a abstract way to represents interval time when the client check or calculate in background some metrics. 
        self.__heartbeat_interval:int                 = HF.parse_timespan(heartbeat_interval)
        # Every X hearbets the client check the availability of the peers
        self.__check_peers_availability_interval:int  = int(HF.parse_timespan(check_peers_availavility_interval)/self.__heartbeat_interval)
        # Peers
        # self.__peers = peers
        # This flag manage the console log level.
        self.__debug = debug
        # Basic lock that help to protect critical sections in the code. 
        # self.__lock = Lock()
        # self.__bucket_id = bucket_id
        
        self.pending_tasks:Dict[str,Task] = {}
        self.completed_tasks:Dict[str, Tuple[Task, Event]] = {}

        self.pending_task_q = Queue(maxsize=q_size)
        self.balancing_q = Queue(maxsize=q_size)
        self.completed_tasks_q = Queue(maxsize=q_size)

        self.peers_healer = PeerHealer(q = self.balancing_q, heartbeat= check_peers_availavility_interval,peers=peers,show_logs=False)
        self.peers_healer.start()

        def console_handler_filter(record:L.LogRecord):
            if disable_log:
                return False
            
            if self.__debug and  record.levelno == L.ERROR or record.levelno == L.DEBUG:
                return True
            elif record.levelno == L.INFO:
                return True
            else:
                return False

        # Log for basic operations
        self.__log         = Log(
            name = self.client_id,
            console_handler_filter = console_handler_filter,
            error_log=True,
            when=log_when,
            interval=log_interval,
            output_path=Some("{}/{}".format(output_path,self.client_id))
        )
        # If the output path does not exists then create it
        if not os.path.exists(output_path):
            os.makedirs(name=output_path,mode=0o777,exist_ok=True)
        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers
        lb_daemon = LoadBalancingDaemon(pending_task_q=self.pending_task_q,balancing_q=self.balancing_q,peer_healer=self.peers_healer)
        lb_daemon.start()
        task_handler = AsyncClientHandler(pending_task_q= self.pending_task_q,lb_q=self.balancing_q,completed_tasks_q = self.completed_tasks_q,peer_healer=self.peers_healer)
        task_handler.start()

    
    def get_task_result(self,task_id:str,interval:str="1s",timeout:str = "24h")->Result[Tuple[Task,Event],Exception]:
        is_running = True
        _interval = HF.parse_timespan(interval)
        _timeout = HF.parse_timespan(timeout)
        start_time = T.time()
        while is_running:
            elapsed_time = T.time() - start_time
            if elapsed_time >= _timeout:
                return Err(Exception("Timeout"))
            if task_id in self.completed_tasks:
                return Ok(self.completed_tasks.pop(task_id) )
            T.sleep(_interval)

    def run(self):
        while True:
            event:Event = self.completed_tasks_q.get(block=True)
            if isinstance(event,Put):
                put:Put = event
                if put.task_id in self.pending_tasks:
                    task = self.pending_tasks[put.task_id]
                    self.completed_tasks[put.task_id] = (task,put)
            if isinstance(event,Get):
                print("_____________RESPONSEE_GET!!!")
                # print("______________________PUT")
    def put(self,bucket_id:str,key:str,path:str,chunk_size:str ="1MB",producer_id:str="mictlanx")->Result[str,Exception]:
        exists = os.path.exists(path=path)
        try:
            if not exists:
                raise Exception("{} not exists".format(path))

            (checksum, size) = XoloUtils.sha256_file(path=path)
            task = PutTask(
                    bucket_id=bucket_id,
                    key= checksum if key =="" else key,
                    checksum=checksum,
                    size=size,
                    path=path,
                    chunk_size=chunk_size,
                    producer_id=producer_id
            )
            self.__log.debug({
                "event":"CREATE.TASK",
                "task_id":task.task_id,
                "task_type": task.task_type,
                "bucket_id":bucket_id,
                "key":key,
                "checksum":checksum,
                "size":size,
                "path":path,
                "chunk_size":chunk_size,
                "producer_id":producer_id
            })

            self.pending_tasks[task.task_id] = task
            self.pending_task_q.put(task)
            return Ok(task.task_id)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
    def get(self,bucket_id:str,key:str,chunk_size:str ="1MB")->Result[str,Exception]:
        # exists = os.path.exists(path=path)
        try:
            pass
            task =  GetTask(
                bucket_id=bucket_id,
                key=key,
                chunk_size=chunk_size
            )
            self.__log.debug({
                "event":"CREATE.TASK",
                "task_id":task.task_id,
                "task_type": task.task_type,
                "bucket_id":bucket_id,
                "key":key,
                "chunk_size":chunk_size,
            })

            self.pending_tasks[task.task_id] = task
            self.pending_task_q.put(task)
            return Ok(task.task_id)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)

# _________________________________________________________________

if __name__ =="__main__":
    # Se crea el cliente con sus parametros
    client=  AsyncClient(
        client_id="client-0",
        peers= [
            Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=7000),
            Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=7001),
        ],
        debug= True,
        max_workers=10,
        lb_algorithm="2CHOICES_UF",
    )
    futures = []
    client.start()
    with ThreadPoolExecutor(max_workers=4) as tp:
        rnd_str = uuid4().hex[:5]
        for i in range(1):

            key = "{}".format(rnd_str,i)
            fut = tp.submit(client.put, 
                bucket_id="b0",
                key=key,
                path="/source/01.pdf",
                chunk_size="1MB",
                producer_id="YO",
            )
            fut1=tp.submit(client.get,
                bucket_id = "b0",
                key = key 
            )
            futures.append(fut)
            futures.append(fut1)
        for x in as_completed(futures):
            result:Result[str,Exception] = x.result()
            if result.is_err:
                pass
            else:
                response = result.unwrap()
                print("RESPONSE_",response)
                y = client.get_task_result(task_id=response)
                if y.is_ok:
                    (task,event) = y.unwrap()
                    print("RESULT",event.arrival_time,event.departure_at,event.service_time,event.waiting_time)
        
            # print(x.result())
    T.sleep(10000)
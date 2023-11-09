
import os
import json as J
import time as T
import requests as R
import hashlib as H
import magic as M
import numpy as np
import numpy.typing as npt
import humanfriendly as HF
from option import Result,Ok,Err,Option,NONE,Some
from typing import List,Dict,Generator,Iterator,Awaitable,Set
from mictlanx.v4.interfaces.responses import PutResponse,GetMetadataResponse,GetBytesResponse,GetNDArrayResponse,Metadata,GetBucketMetadataResponse
from mictlanx.logger.log import Log
from threading import Thread,Lock
from concurrent.futures import ThreadPoolExecutor,as_completed
from itertools import chain
from functools import reduce
from mictlanx.v4.interfaces.index import Peer,BallContext,PeerStats
from mictlanx.utils.segmentation import Chunks
from collections import deque
from mictlanx.v4.xolo.utils import Utils as XoloUtils
import logging as L

API_VERSION = 4 




class Client(object):
    # recommend worker is 4 
    def __init__(
            self,
            client_id:str,
            bucket_id:str = "MICTLANX",
            peers:List[Peer] = [],
            debug:bool=True,
            show_metrics:bool=True,
            daemon:bool=True,
            max_workers:int = 12,
            lb_algorithm:str="ROUND_ROBIN",
            output_path:str = "/mictlanx/client",
            heartbeat_interval:str  = "5s",
            metrics_buffer_size:int =  100,
            check_peers_availavility_interval:str = "1m",
            disable_log:bool = False,
            log_when:str="m",
            log_interval:int = 10
    ):
        # Client unique identifier
        self.client_id = client_id
        # Heartbeats are a abstract way to represents interval time when the client check or calculate in background some metrics. 
        self.__heartbeat_interval:int                 = HF.parse_timespan(heartbeat_interval)
        # Every X hearbets the client check the availability of the peers
        self.__check_peers_availability_interval:int  = int(HF.parse_timespan(check_peers_availavility_interval)/self.__heartbeat_interval)
        # This variable show the metrics table if True
        self.__show_metrics:bool = show_metrics
        # Load balancing algorithm
        self.__lb_algorithm     = lb_algorithm
        # Total number of put operations
        self.__put_counter = 0
        # Total number of get operations
        self.__get_counter = 0
        # Peers
        self.__peers = peers
        # This flag manage the console log level.
        self.__debug = debug
        # Basic lock that help to protect critical sections in the code. 
        self.__lock = Lock()
        self.__bucket_id = bucket_id
        
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
            interval=log_interval
        )
        #  Special log that save only the metrics
        self.__log_metrics = Log(
            name = "{}.metrics".format(self.client_id),
            console_handler_filter = console_handler_filter, 
            when=log_when,
            interval=log_interval
        )
        self.__log_access = Log(
            name = "{}.access".format(self.client_id),
            console_handler_filter = console_handler_filter, 
            when=log_when,
            interval=log_interval
        )

        # Last interarrival 
        self.__put_last_interarrival = 0
        # Sum of all arrival_times
        self.__put_interarrival_time_sum:int = 0
        # 
        self.__get_interarrival_time_sum = 0
        self.__get_last_interarrival = 0
        self.__daemon = daemon
        
        # get current stats from peers
        # PeerID -> PeerStats
        self.__peer_stats:Dict[str, PeerStats] = {}
        self.__check_stats_peers(self.__peers)

        self.__put_response_time_dequeue = deque(maxlen=metrics_buffer_size)
        self.__get_response_time_dequeue = deque(maxlen=metrics_buffer_size)

        # self

        # If the output path does not exists then create it
        if not os.path.exists(output_path):
            os.makedirs(name=output_path,mode=0o777,exist_ok=True)
        

        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers
        # PeerID -> u64
        self.__put_operations_per_peer= {}
        # PeerID -> u64
        self.__access_total_per_peer= {}
        
        # PeerID.Key -> u64
        self.__replica_access_counter= {}
        
        # PeerId -> List[Key]
        self.__keys_per_peer:Dict[str,Set[str]] = {}
        # Key -> BallContext
        self.__balls_contexts:Dict[str,BallContext] = {}
        # BallID -> List[BallContext]
        self.__chunk_map:Dict[str,List[BallContext]] = {}
        # PeerID -> f32
        # self.disk_ufs:Dict[str, float] = {}
        self.__max_workers = max_workers
        self.__thread_pool = ThreadPoolExecutor(max_workers= self.__max_workers,thread_name_prefix="mictlanx-worker")
        # self.__timeout     = 60*2

        # self.enable_daemon = True

        if self.__daemon:
            self.__thread     = Thread(target=self.__run,name="mictlanx-metrics-0")
            self.__thread.setDaemon(True)
            self.__thread.start()


    # Return the unavailible peers
    def __check_stats_peers(self, peers:List[Peer])->List[Peer]:
        counter = 0
        unavailable_peers =[]
        for peer in peers:
            get_ufs_response = peer.get_ufs()
            if get_ufs_response.is_ok:
                response = get_ufs_response.unwrap()
                peer_stats = PeerStats(peer_id=peer.peer_id)
                peer_stats.total_disk = response.total_disk
                peer_stats.used_disk  = response.used_disk
                # self.__log.debug("{}".format(peer_stats))
                self.__peer_stats[peer.peer_id] = peer_stats
                counter +=1
                self.__log.debug("Peer {} is  available".format(peer.peer_id))
            else:
                unavailable_peers.append(peer)
                self.__log.error("Peer {} is not available.".format(peer.peer_id))
                
                
        percentage_available_peers =  (counter / len(peers))*100 
        if percentage_available_peers == 0:
            self.__log.error("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
            raise Exception("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
        self.__log.debug("{}% of the peers are available".format(percentage_available_peers ))
        return unavailable_peers
            

                # peer_stats.disk_uf    = 
            # print(peer.peer_id, get_ufs_response)

    def __global_operation_counter(self):
        return self.__get_counter + self.__put_counter

    def __lb(self,operation_type:str,algorithm:str="ROUND_ROBIN",key:str="",size:int= 0,peers:List[str]=[])->Peer:
        try:
            filtered_peers = list(filter(lambda x: x.peer_id in peers,self.__peers))
            if filtered_peers == 1:
                return filtered_peers[0]
            
            if algorithm =="ROUND_ROBIN":
                return self.__lb_rb(operation_type,peers=filtered_peers)
            elif algorithm == "HASH":
                return self.__lb_hash(operation_type,key=key, peers=filtered_peers)
            elif algorithm == "PSEUDORANDOM":
                return self.__lb_pseudo_random(operation_type,key=key, peers=filtered_peers)
            elif algorithm == "2CHOICES":
                return self.__lb__two_choices(operation_type,key=key, peers=filtered_peers)
            elif algorithm == "SORT_UF":
                return self.__lb_sort_uf(operation_type = operation_type , key= key,size=size, peers=filtered_peers)
            elif algorithm == "2CHOICES_UF":
                return self.__lb_2choices_uf(operation_type = operation_type , key= key,size=size, peers=filtered_peers)
            else:
                return self.__lb_rb(operation_type,peers=filtered_peers)
        except Exception as e:
            self.__log.error("LB_ERROR "+str(e))

    def __lb_sort_uf(self,operation_type:str,key:str, size:int,peers:List[Peer])->Peer:
        peers_ids        = list(map(lambda x : x.peer_id ,peers))
        peers_stats      = dict(list(filter(lambda x: x[0] in peers_ids , self.__peer_stats.items())))
        ufs_peers        = dict(list(map(lambda x: (x[0], x[1].calculate_disk_uf(size=size)), peers_stats.items())))
        sorted_ufs_peers = sorted(ufs_peers.items(), key=lambda x: x[1])
        selected_peer_id = sorted_ufs_peers[0][0]
        selected_peer    = next((peer  for peer in peers if peer.peer_id == selected_peer_id),None)
        return selected_peer
        
    def __lb_2choices_uf(self,operation_type:str,key:str, size:int,peers:List[Peer])->Peer:
        if operation_type == "PUT":
            peers_ids        = list(map(lambda x : x.peer_id ,peers))
            peers_stats      = dict(list(filter(lambda x: x[0] in peers_ids , self.__peer_stats.items())))
            ufs_peers        = list(map(lambda x: (x[0], x[1].calculate_disk_uf(size=size)), peers_stats.items()))
            peer_x_index     = np.random.randint(low= 0, high=len(ufs_peers))
            peer_y_index     = np.random.randint(low= 0, high=len(ufs_peers))
            max_tries        = len(ufs_peers)
            # print("<<<<<MAX_TRIES>>>>",max_tries)
            i                = 0
            if not max_tries == 2:
                while peer_x_index == peer_y_index and i < max_tries :
                    peer_y_index           = np.random.randint(low= 0, high=len(ufs_peers))
                    i += 1
            else:
                peer_x_index = 0
                peer_y_index = 1
            # ______________________________
            peer_x = ufs_peers[peer_x_index]
            peer_y = ufs_peers[peer_y_index]
            # print("UF_PEERS", ufs_peers)
            
            if peer_x[1] < peer_y[1]:
                selected_peer    = next((peer  for peer in peers if peer.peer_id == peer_x[0]),None)
                return selected_peer
            else:
                selected_peer    = next((peer  for peer in peers if peer.peer_id == peer_y[0]),None)
                return selected_peer
        else:
            return self.__lb__two_choices(operation_type=operation_type,peers=peers)

        
    def __lb__two_choices(self,operation_type:str,peers:List[Peer])-> Peer: 
        x = np.random.randint(0,len(peers))
        y = np.random.randint(0,len(peers))
        max_tries = len(peers)
        i = 0
        while x == y and i < max_tries:
            x = np.random.randint(0,len(peers))
            y = np.random.randint(0,len(peers))
            i+=1



        peer_x = peers[x]
        peer_y = peers[y]

        # <This should be a function to return a peer based on whatever number maybe select a peer crunching numbers using characteristis of the data or data about the system.
        if operation_type == "PUT":
            peer_x_put_counter = self.__put_operations_per_peer.get(peer_x.peer_id,0)
            peer_y_put_counter = self.__put_operations_per_peer.get(peer_y.peer_id,0)
            if peer_x_put_counter < peer_y_put_counter:
                return peer_x
            else:
                return peer_y
        elif operation_type == "GET":
            peer_x_get_counter = self.__access_total_per_peer.get(peer_x.peer_id,0)
            peer_y_get_counter = self.__access_total_per_peer.get(peer_y.peer_id,0)
            if peer_x_get_counter < peer_y_get_counter:
                return peer_x
            else:
                return peer_y
        else:
            return peer_x



        

        # peers
    def __lb_rb(self,operation_type:str,peers:List[Peer]=[])->Peer:
        x = self.__global_operation_counter()
        return peers[x % len(peers)]
    
    def __lb_hash(self,key:str, peers:List[Peer]=[])->Peer:
        return peers[hash(key) % len(peers)]

    def __lb_pseudo_random(self,key:str, peers:List[Peer]=[])->Peer:
        return peers[np.random.randint(0,len(peers))]


    def shutdown(self):
        self.__log.debug("SHUTDOWN {}".format(self.client_id))
        # if self.enable_daemon:
        self.__thread_pool.shutdown(wait=True)
        if self.__daemon:
            self.enable_daemon=False
            self.__thread.join()

    def __run(self):
        beats_counter = 0
        # sync_peers_threshold = 10
        while self.__daemon:
            T.sleep(self.__heartbeat_interval)
            global_counter = self.__put_counter + self.__get_counter 
            if global_counter >0:
                put_avg_iat = self.__put_interarrival_time_sum / (self.__put_counter) if self.__put_counter >0 else 0
                get_avg_iat = self.__get_interarrival_time_sum / (self.__get_counter) if self.__get_counter else 0
                global_avg_iat = (self.__get_interarrival_time_sum + self.__put_interarrival_time_sum) / global_counter
            else:
                put_avg_iat = 0
                get_avg_iat = 0
                global_avg_iat = 0
            
            avg_put_response_time = np.array(self.__put_response_time_dequeue).mean() if not len(self.__put_response_time_dequeue)==0 else 0.0
            avg_get_response_time = np.array(self.__get_response_time_dequeue).mean() if not len(self.__get_response_time_dequeue)==0 else 0.0
            avg_global_response_time = np.array(self.__get_response_time_dequeue+self.__put_response_time_dequeue).mean() if not len(self.__get_response_time_dequeue)==0 and not len(self.__put_response_time_dequeue) else 0.0

            # if self.__debug:
            if self.__show_metrics:
                print("_"*50)
                title = " ┬┴┬┴┤┬┴┬┴┤ MictlanX - Daemon ►_◄  ┬┴┬┴┤┬┴┬┴┤"
                print("|{:^50}|".format(title))
                print("-"*52)
                print(f"| {'PUT_COUNTER ':<43}| {'{}'.format(self.__put_counter):<4}|")
                print(f"| {'GET_COUNTER ':<43}| {'{}'.format(self.__get_counter):<4}|")
                print(f"| {'GLOBAL_COUNTER ':<43}| {'{}'.format(global_counter):<4}|")
                put_avg_iat_formatted = "{:.2f}".format(put_avg_iat)
                get_avg_iat_formatted = "{:.2f}".format(get_avg_iat)
                global_avg_iat_formatted = "{:.2f}".format(global_avg_iat)
                print(f"| {'AVG_PUT_INTERARRIVAL ':<43}| {'{}'.format(put_avg_iat_formatted):<4}|")
                print(f"| {'AVG_GET_INTERARRIVAL ':<43}| {'{}'.format(get_avg_iat_formatted):<4}|")
                print(f"| {'AVG_GLOBAL_INTERARRIVAL ':<43}| {'{}'.format(global_avg_iat_formatted):<4}|")
                put_avg_rt_formatted = "{:.2f}".format(avg_put_response_time)
                get_avg_rt_formatted = "{:.2f}".format(avg_get_response_time)
                global_avg_rt_formatted = "{:.2f}".format(avg_global_response_time)
                print(f"| {'AVG_PUT_RESPONSE_TIME':<43}| {'{}'.format(put_avg_rt_formatted):<4}|")
                print(f"| {'AVG_GET_RESPONSE_TIME':<43}| {'{}'.format(get_avg_rt_formatted):<4}|")
                print(f"| {'AVG_GLOBAL_RESPONSE_TIME':<43}| {'{}'.format(global_avg_rt_formatted ):<4}|")
                print("-" * 52)
                global_stats_map = {
                    "put_counter":self.__put_counter,
                    "get_counter":self.__get_counter,
                    "global_counter":global_counter,
                    "avg_put_iat":put_avg_iat_formatted,
                    "avg_get_iat":get_avg_iat_formatted,
                    "avg_global_iat":global_avg_iat_formatted,
                    "avg_put_response_time":avg_put_response_time,
                    "avg_get_response_time":avg_get_response_time,
                    "avg_global_response_time":avg_global_response_time,

                }
                # print("GLOBAL_STATS",global_stats_map)
                self.__log_metrics.info(global_stats_map)
                print("_"*30)
                # print("PRINT!!! LOG_METRICs")
                for k,peer_stats in self.__peer_stats.items():
                    self.__log_metrics.info({
                        "peer_id": peer_stats.get_id(),
                        "put_counter":peer_stats.put_counter,
                        "get_counter":peer_stats.get_counter,
                        "global_counter":peer_stats.global_counter(),
                        "total_disk":peer_stats.total_disk,
                        "used_disk":peer_stats.used_disk,
                        "available_disk":peer_stats.available_disk(),
                        "put_frecuency":peer_stats.put_frequency(),
                        "get_frecuency":peer_stats.get_frequency(),
                        "disk_uf":peer_stats.calculate_disk_uf()
                    })
                    print("_"*30)
            
            if beats_counter % self.__check_peers_availability_interval == 0:
                self.__peers_availability_check()
            beats_counter += 1
    
    def __peers_availability_check(self):
        unavailable_peers     = self.__check_stats_peers(peers=self.__peers)
        unavailable_peers_ids = list(map(lambda x: x.peer_id, unavailable_peers))
        filtered              = list(filter(lambda peer: not peer.peer_id in unavailable_peers_ids ,self.__peers))
        if not len(unavailable_peers_ids) == 0:
            with self.__lock:
                # Update the peers only availables ones
                self.__peers = filtered
                # Delete all unavailable peers 
                for unavailable_peer_id in unavailable_peers_ids:
                    if unavailable_peer_id in self.__peer_stats:
                        self.__log.debug({"event":"DELETE_PEER", "peer_id": unavailable_peer_id})
                        del self.__peer_stats[unavailable_peer_id]
                    

    # PUT
    def put(self,value:bytes,tags:Dict[str,str]={},checksum_as_key:bool=True,key:str="",ball_id:str ="",bucket_id:str="",timeout:int = 60*2,peer_id:Option[str]=NONE, disabled:bool=False)-> Awaitable[Result[PutResponse,Exception]]:
        return self.__thread_pool.submit(self.__put,key=key, value=value, tags=tags,checksum_as_key=checksum_as_key,ball_id = key if ball_id == "" else ball_id,bucket_id=self.__bucket_id if bucket_id == "" else bucket_id,timeout=timeout,peer_id=peer_id,disabled=disabled)
    def __put(self,
              value:bytes,
              tags:Dict[str,str]={},
              checksum_as_key=True,
              key:str="",
              ball_id:str="",
              bucket_id:str="",
              timeout:int = 60*2,
              peer_id:Option[str]= NONE,
              disabled:bool = False,
    )->Result[PutResponse,Exception]:
        try:
            # The arrivla time of the put operations
            start_time = T.time()
            # atomic increasing the number of put operations.... basically a counter
            with self.__lock:
                self.__put_counter += 1
                # Check if the   (This must a put to a queue data structure.) 
                self.__put_last_interarrival = start_time
                if not self.__put_last_interarrival == 0:
                    # At_(i-1)  - At_i
                    interarrival = start_time - self.__put_last_interarrival 
                    # Sum the current interarrival
                    self.__put_interarrival_time_sum += interarrival
                    # self.put_last_interarrival = start_time
            
            # Check if the checksum field exists in the tags dictionary then...
            ## if the checksum exists first check is the actual value of the checksum is not empty string if not then assign the value to a variable checksum
            ### If the checksum is empty then calculated using the value
            #### If not is in the tags then calculated 
            checksum = None
            if "checksum" in tags:
                
                if tags["checksum"] != "":
                    checksum = tags["checksum"]
                else:
                    checksum = XoloUtils.sha256(value= value)
            else:
                checksum = XoloUtils.sha256(value= value)
            # 
            
            # if the flag checksum as key is False and the key is not empty then use key parameter else checksum. 
            key     = key if (not checksum_as_key or not key =="")  else checksum
            # if the ball_id is empty then use the key as ball_id.
            ball_id = key if ball_id == "" else ball_id
            # 
            size    = len(value)

            with self.__lock:
                if peer_id.is_some:
                    _peers = [peer_id.unwrap()]
                else:
                    _peers = list(map(lambda x: x.peer_id,self.__peers))
                # ______________________________________________________
                # print(_peers)
                peer    = self.__lb(
                    operation_type = "PUT",
                    algorithm      = self.__lb_algorithm,
                    key            = key,
                    peers          = _peers,
                    size           = size
                )
            # print(peer)
            # peer.unwrap

            content_type        = M.from_buffer(value,mime=True)
            if content_type == "application/octet-stream":
                # Double check
                c = M.from_buffer(value[:2048], mime=True)
                if not c =="application/octet-stream":
                    content_type = c

                
            
            # print("DISABLED",disabled)
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            put_metadata_result = peer.put_metadata(
                key          = key, 
                size         = size, 
                checksum     = checksum,
                tags         = tags,
                producer_id  = self.client_id,
                content_type = content_type,
                ball_id      = ball_id,
                bucket_id    = _bucket_id,
                timeout      = timeout,
                is_disable   = disabled
            )
            if put_metadata_result.is_err:
                raise put_metadata_result.unwrap_err()
            put_metadata_response = put_metadata_result.unwrap()

            put_response = peer.put_data(task_id= put_metadata_response.task_id, key= key, value= value, content_type=content_type,timeout=timeout)
            if put_response.is_err:
                raise put_response.unwrap_err()
            
            response_time = T.time() - start_time
            self.__put_response_time_dequeue.append(response_time)
            self.__log.info({
                "event":"PUT",
                "bucket_id":bucket_id,
                "key":key,
                "size":size,
                "response_time":response_time,
                "peer_id":peer.peer_id
            })
            
            # _____________________________
            res = PutResponse(
                key           = key,
                response_time = response_time,
                throughput    = float(size) / float(response_time),
                node_id       =peer.peer_id
            )

            with self.__lock:
                self.__log_access.info({
                    "event":"PUT",
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":size,
                    "peer_id":peer.peer_id
                })
                _peer_id  = peer.peer_id
                self.__peer_stats[_peer_id].put(key=key,size=size)
                if not _peer_id in self.__put_operations_per_peer:
                    self.__put_operations_per_peer[_peer_id] = 1 
                else:
                    self.__put_operations_per_peer[_peer_id] += 1
                #  UPDATE balls_contexts
                # _________________________________________________________________________________
                if not key in self.__balls_contexts:
                    self.__balls_contexts[key] = BallContext(size=size, locations=set([_peer_id]))
                    self.__keys_per_peer[_peer_id] = set([key])
                else:
                    self.__keys_per_peer[_peer_id].add(key)
                    self.__balls_contexts[key].locations.add(_peer_id)
                
                if not ball_id in self.__chunk_map:
                    self.__chunk_map[ball_id] = [ BallContext(size=size, locations=set([_peer_id]))]
                else:
                    self.__chunk_map[ball_id].append(BallContext(size=size, locations=set([_peer_id])))

            
            return Ok(res)
            # print("PUT_RESPONSE",put_data_response.service_time+put_metadata_response.service_time,put_data_response.throughput)
            
        except R.RequestException as e:
            self.__log.error(str(e))
            with self.__lock:
                self.__put_counter-=1

            if isinstance(e, R.RequestException):
                response = e.response
                if hasattr(response, "headers"):
                    headers = response.headers 
                    error_msg = headers.get("error-message","UKNOWN_ERROR")
                    self.__log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.__log.error(str(e))
            with self.__lock:
                self.__put_counter-=1
            return Err(e)
    def put_ndarray(self, key:str, ndarray:npt.NDArray,tags:Dict[str,str],bucket_id:str="",timeout:int = 60*2,peer_id:Option[str]=NONE)->Awaitable[Result[PutResponse,Exception]]:
        try:
            value:bytes = ndarray.tobytes()
            dtype       = str(ndarray.dtype)
            shape_str   = str(ndarray.shape)
            return self.put(key=key, value=value,tags={**tags,"dtype":dtype,"shape":shape_str },bucket_id=bucket_id,timeout=timeout, peer_id=peer_id)
        except R.RequestException as e:
            self.__log.error(str(e))
            if isinstance(e, R.RequestException):
                response = e.response
                if hasattr(response, "headers"):
                    headers = response.headers 
                    error_msg = headers.get("error-message","UKNOWN_ERROR")
                    self.__log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.__log.error(str(e))
            return Err(e)

    def put_chunks(self,
                   key:str,
                   chunks:Chunks,
                   tags:Dict[str,str],
                   checksum_as_key:bool= False,
                   bucket_id:str="",
                   timeout:int = 60*2,
                   peers_ids:List[str] = []
    )->Generator[Result[PutResponse,Exception],None,None]:
        futures:List[Awaitable[Result[PutResponse,Exception]]] = []
        
        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        max_peers_ids = len(peers_ids)
        
        for i,chunk in enumerate(chunks.iter()):
            if max_peers_ids == 0:
                peer_id = NONE
            else:
                peer_id = peers_ids[i%max_peers_ids]
            fut = self.put(
                value=chunk.data,
                tags={**tags, **chunk.metadata, "index": str(chunk.index), "checksum":chunk.checksum},
                key=chunk.chunk_id,
                checksum_as_key=checksum_as_key,
                ball_id=key,
                bucket_id=_bucket_id,
                timeout=timeout,
                peer_id=peer_id
            )
            futures.append(fut)
        
        for result in as_completed(futures):
            res = result.result()
            if res.is_ok:
                chunk_ctx = BallContext(size= chunk.size, locations=[ res.map(lambda x: x.node_id).unwrap() ])
                if key in self.__chunk_map:
                    self.__chunk_map[key].append(chunk_ctx)
                else:
                    self.__chunk_map[key] =[chunk_ctx]
            
            yield res

    # GET
    
    def __get_bucket_metadata(self,bucket_id:str, peer:Peer=NONE, timeout:int = 60*2)->Result[GetBucketMetadataResponse,Exception]: 
        try:
            start_time = T.time()
            x = peer.get_bucket_metadata(bucket_id=bucket_id,timeout=timeout)
            if x.is_ok:
                service_time = T.time() - start_time
                self.__log.info({
                    "event":"GET_BUCKET_METADATA",
                    "bucket_id":bucket_id,
                    "peer_id":peer.peer_id,
                    "service_time":service_time
                })
                return x
            else:
                service_time = T.time() - start_time
                self.__log.error({
                    "msg":str(x.unwrap_err()),
                    "bucket_id":bucket_id,
                    "peer_id":peer.peer_id,
                    "service_time":service_time
                })
                return x
                
        except Exception as e:
            return Err(e)

    def get_bucket_metadata(self,bucket_id:str, peer:Option[Peer]=NONE, timeout:int = 60*2)->Awaitable[Result[GetBucketMetadataResponse, Exception]]:
        start_time = T.time()
        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        try:
            if peer.is_none:
                _peer = self.__lb(
                    operation_type = "GET",
                    algorithm      = "ROUND_ROBIN",
                    key            = _bucket_id,
                    peers          = list(map(lambda x: x.peer_id,self.__peers))
                )
            else:
                _peer = peer.unwrap()
            x     = self.__thread_pool.submit(self.__get_bucket_metadata, bucket_id=_bucket_id,timeout=timeout, peer= _peer)
            # service_time = T.time() - start_time
            return x
        except Exception as e:
            return Err(e)


    def get_ndarray(self, key:str,bucket_id:str="",timeout:int= 60*2)->Awaitable[Result[GetNDArrayResponse,Exception]]:
        start_time = T.time()
        try:
            
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            def __inner()->Result[GetNDArrayResponse,Exception]:
                get_result =  self.__get(key=key,timeout=timeout,bucket_id=_bucket_id)
                if get_result.is_ok:
                    get_response = get_result.unwrap()
                    metadata     = get_response.metadata
                    shape        = eval(metadata.tags["shape"])
                    dtype        = metadata.tags["dtype"]
                    ndarray      = np.frombuffer(get_response.value,dtype=dtype).reshape(shape)
                    response_time = T.time() - start_time
                    return Ok(GetNDArrayResponse(value=ndarray, metadata=metadata, response_time=response_time))
                else:
                    return get_result
                
            # get_future_result =  self.get(key=key,timeout=timeout)
            return self.__thread_pool.submit(__inner)
                # get_result.add_done_callback(Client.fx)
        except KeyError as e :
            self.__log.error(str(e))
            return Err(e)
        except R.RequestException as e:
            self.__log.error(str(e))

            if isinstance(e, R.RequestException):
                response = e.response
                if hasattr(response, "headers"):
                    headers = response.headers 
                    error_msg = headers.get("error-message","UKNOWN_ERROR")
                    self.__log.error("{}".format(error_msg))
            # if isinstance(e, R.RequestException):
            #     headers = e.response.headers | {}
            #     error_msg = headers.get("error-message","UKNOWN_ERROR")
            #     self.__log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.__log.error(str(e))
            return Err(e)

    def __get_metadata(self,key:str, peer:Peer,bucket_id:str="", timeout:int = 60*2):
        try:
            start_time = T.time()
            get_metadata_response = R.get("{}/api/v{}/buckets/{}/metadata/{}".format(peer.http_url(),API_VERSION,bucket_id,key), timeout=timeout)
            get_metadata_response.raise_for_status()
            response = GetMetadataResponse(**get_metadata_response.json() )
            response_time = T.time() - start_time
            self.__log.info({
                    "event":"GET_METADATA",
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":response.metadata.size, 
                    "response_time":response_time,
                    "peer_id":peer.peer_id
            })
            return Ok(response)
        except Exception as e:
            return Err(e)
    def get_metadata(self,key:str,bucket_id:str="",peer:Option[Peer]=NONE ,timeout:int=60*2) -> Awaitable[Result[GetMetadataResponse, Exception]] :
        if peer.is_none:
            _peer = self.__lb(
                    operation_type="GET",
                    algorithm=self.__lb_algorithm,
                    key=key,
                    size=0,
                    peers=list(map(lambda x: x.peer_id,self.__peers))
            )
        else:
            _peer = peer.unwrap()
        
        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        return self.__thread_pool.submit(self.__get_metadata, key = key, peer = _peer, timeout = timeout,bucket_id=_bucket_id)
       
    def get (self,key:str,bucket_id:str="",timeout:int = 60*2)->Awaitable[Result[GetBytesResponse,Exception]]:

        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        x = self.__thread_pool.submit(self.__get, key = key,timeout=timeout,bucket_id=_bucket_id)
        return x

    def __get(self,key:str,bucket_id:str="",timeout:int=60*2)->Result[GetBytesResponse,Exception]:
        # print("HJEREE GET")
        try:
            start_time = T.time()
            
            bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            # Metrics. 
            with self.__lock:
                if self.__get_last_interarrival == 0:
                    self.__get_last_interarrival = start_time
                else:
                    self.__get_interarrival_time_sum += (start_time - self.__get_last_interarrival )
                    self.__get_last_interarrival = start_time
                
                if not key in self.__balls_contexts:
                    peer = self.__lb(
                        operation_type = "GET",
                        algorithm      = self.__lb_algorithm,
                        key            = key,
                        size           = 0,
                        peers          = list(map(lambda x: x.peer_id,self.__peers))
                    )
                    self.__log_access.info({
                        "event":"MISS",
                        "bucket_id":bucket_id,
                        "key":key,
                        "peer_id": peer.peer_id
                    })
                else:
                    locations = self.__balls_contexts[key].locations
                    peer      = self.__lb(operation_type="GET", algorithm=self.__lb_algorithm, key=key, size=0, peers=locations)
                    self.__log_access.info({
                        "event":"HIT",
                        "bucket_id":bucket_id,
                        "key":key,
                        "peer_id": peer.peer_id
                    })
            
            # _____________________________________________________________________________________________________________________
            get_metadata_result:Result[GetMetadataResponse,Exception] = self.__get_metadata(bucket_id=bucket_id,key= key, peer=peer,timeout=timeout)
            # .result()
            if get_metadata_result.is_err:
                raise Exception(str(get_metadata_result.unwrap_err()))
            metadata_response = get_metadata_result.unwrap()
            metadata_service_time = T.time() - start_time
            # _________________________________________________________________________
            # get_metadata_response = R.get("{}/api/v{}/metadata/{}".format(peer.http_url(),API_VERSION,key), timeout=timeout)
            # get_metadata_response.raise_for_status()
            # metadata_response = GetMetadataResponse(**get_metadata_response.json() )
            # _____________________________________________________________________________
            if metadata_response.node_id != peer.peer_id:
                peer = next(filter(lambda p: p.peer_id == metadata_response.node_id,self.__peers),Peer.empty())
                if(peer.port == -1):
                    raise Exception("{} not found".format(key))
                    # return Err(Exception())
            
            # if not key in self.balls_contexts:
                # self
            response = R.get("{}/api/v{}/buckets/{}/{}".format(peer.http_url(),API_VERSION,bucket_id,key),timeout=timeout)
            response.raise_for_status()
            response_time = T.time() - start_time
            # 
            self.__get_response_time_dequeue.append(response_time)
            self.__log.info(
                {
                    "event":"GET",
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":metadata_response.metadata.size, 
                    "response_time":response_time,
                    "metadata_service_time":metadata_service_time,
                    "peer_id":peer.peer_id
                }
                # "{} {} {} {}".format("GET", key,metadata_response.metadata.size,response_time)
                )

            # Critical section: Updating peer stats and replica access counter.
            with self.__lock:
                self.__peer_stats[peer.peer_id].get(key=key,size=metadata_response.metadata.size)       
                self.__get_counter+=1
                # this is to construct the replica access matrix
                combined_key = "{}.{}".format(peer.peer_id,key)
                if not combined_key in self.__replica_access_counter:
                    self.__replica_access_counter[combined_key] = 1
                else:
                    self.__replica_access_counter[combined_key] +=1

                if not peer.peer_id in self.__access_total_per_peer:
                    self.__access_total_per_peer[peer.peer_id] = 1 
                else:
                    self.__access_total_per_peer[peer.peer_id] += 1
                
                self.__log_access.info({
                    "event":"GET",
                    "peer_id": peer.peer_id,
                    "bucket_id":bucket_id,
                    "key":key,
                    "current_gets":self.__replica_access_counter[combined_key],
                    "total_gets": self.__access_total_per_peer[peer.peer_id],
                    "combined_key":combined_key,
                })
                
                #  CHECK IF KEY IS NOT IN BALL CONTEXT then added using the metadata
                if not key in self.__balls_contexts:
                    self.__balls_contexts[key] = BallContext(size=metadata_response.metadata.size, locations=set([peer.peer_id] ))
                    self.__keys_per_peer[peer.peer_id] = set([key])
                else:
                    if not metadata_response.node_id in self.__balls_contexts[key].locations:
                        self.__balls_contexts[key].locations.add(peer.peer_id)
                    

                    if not metadata_response.node_id in self.__keys_per_peer.get(metadata_response.node_id,set([])):
                        self.__keys_per_peer[metadata_response.node_id] = set([key])
                

            return Ok(GetBytesResponse(value=response.content,metadata=metadata_response.metadata,response_time=response_time))

        except R.RequestException as e:
            self.__log.error(str(e))


            if isinstance(e, R.RequestException):
                response = e.response
                if hasattr(response, "headers"):
                    headers = response.headers 
                    error_msg = headers.get("error-message","UKNOWN_ERROR")
                    self.__log.error("{}".format(error_msg))
            # if isinstance(e, R.RequestException):
            #     headers = e.response.headers | {}
            #     error_msg = headers.get("error-message","UKNOWN_ERROR")
            #     self.__log.error("{}".format(error_msg))
            # headers = e.response.headers | {}
            # error_msg = headers.get("error-message","UKNOWN_ERROR")
            # self.__log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.__log.error(str(e))
            return Err(e)

    def get_chunks_metadata(self,key:str,peer:Peer,bucket_id:str="",timeout:int= 60*2)->Result[Iterator[Metadata],Exception]:
        try:
            response = R.get("{}/api/v{}/buckets/{}/metadata/{}/chunks".format(peer.http_url(),API_VERSION,bucket_id,key),timeout=timeout)
            response.raise_for_status()
            chunks_metadata_json = map(lambda x: Metadata(**x) ,response.json())
            return Ok(chunks_metadata_json)
        except R.RequestException as e:
            self.__log.error(str(e))

            if isinstance(e, R.RequestException):
                response = e.response
                if hasattr(response, "headers"):
                    headers = response.headers 
                    error_msg = headers.get("error-message","UKNOWN_ERROR")
                    self.__log.error("{}".format(error_msg))
            # if isinstance(e, R.RequestException):
            #     headers = e.response.headers | {}
            #     error_msg = headers.get("error-message","UKNOWN_ERROR")
            #     self.__log.error("{}".format(error_msg))
            # headers = e.response.headers
            # error_msg = headers.get("error-message","UKNOWN_ERROR")
            # self.__log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.__log.error(str(e))
            return Err(e)
        
        # pass

    def get_and_merge_ndarray(self,key:str,bucket_id:str="",timeout:int= 60*2) -> Awaitable[Result[GetNDArrayResponse,Exception]]:
        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        return self.__thread_pool.submit(self.__get_and_merge_ndarray, key = key,timeout=timeout,bucket_id=_bucket_id)
    
    def __get_and_merge_ndarray(self,key:str,bucket_id:str="",timeout:int=60*2)->Result[GetNDArrayResponse,Exception] :
        start_time = T.time()
        res:Result[GetBytesResponse,Exception] = self.__get_and_merge(bucket_id=bucket_id,key=key,timeout=timeout)
        if res.is_ok:
            response = res.unwrap()
            shapes_str = map( lambda x: eval(x), J.loads(response.metadata.tags.get("shape","[]")) )
            dtype_str  = next(map(lambda x: x,J.loads(response.metadata.tags.get("dtype","[]"))), "float32" )
            shapes_str = list(shapes_str)
            shape = list(shapes_str[0][:])
            shape[0] = 0
            for ss in shapes_str:
                shape[0]+= ss[0]
            ndarray       = np.frombuffer(response.value,dtype=dtype_str).reshape(shape)
            response_time = T.time() - start_time
            response.metadata.tags["shape"] = str(shape)
            response.metadata.tags["dtype"] = dtype_str
            return Ok(GetNDArrayResponse(value=ndarray, metadata=response.metadata, response_time=response_time))
        else:
            return res
    
    def __get_metadata_peers_async(self,key:str,bucket_id:str="",timeout:int = 60*2)->Generator[List[Metadata],None,None]:
        if not key in self.__chunk_map:
            for peer in self.__peers:
                metadata_result = self.get_chunks_metadata(bucket_id=bucket_id,key=key,peer=peer,timeout=timeout)
                if metadata_result.is_ok:
                    yield metadata_result.unwrap()
        else:
            balls_ctxs:List[BallContext] = self.__chunk_map.get(key,[])
            for ball_ctx in balls_ctxs:
                locations = ball_ctx.locations
                # flag      = False
                # while not flag:
                selected_location = self.__lb(operation_type="GET",algorithm=self.__lb_algorithm, key=key, size=0, peers=locations)
                peer              = next(filter(lambda x: x.peer_id == selected_location.peer_id, self.__peers),  Peer.empty())
                if peer.port == -1:
                    raise Exception("Peer not found in __peers")

                metadata_result   = self.get_chunks_metadata(bucket_id=bucket_id,key=key, peer=peer)
                if metadata_result.is_ok:
                    yield metadata_result.unwrap()

    def __get_metadata_valid_index(self,key:str,bucket_id:str="",timeout:int = 60*2)->Generator[Metadata,None,None]: 
        # ______________________________________________________________

        metadatas  = self.__get_metadata_peers_async(bucket_id=bucket_id,key=key,timeout=timeout)
        reduced_metadatas = reduce(lambda a,b: chain(a,b) ,metadatas)
        current_indexes   = []
        # ______________________________________________________________
        for chunk_metadata in reduced_metadatas:
            index = int(chunk_metadata.tags.get("index","-1"))
            if index == -1:
                continue
            if not index in current_indexes:
                current_indexes.append(index)
                yield chunk_metadata

    def get_and_merge_with_num_chunks(self, key:str,num_chunks:int,bucket_id:str = "",timeout:int = 60*2)->Awaitable[Result[GetBytesResponse,Exception]]:

        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        return self.__thread_pool.submit(self.__get_and_merge_with_num_chunks, key = key,timeout=timeout,num_chunks= num_chunks, bucket_id = _bucket_id)
    
    def __get_and_merge_with_num_chunks(self,key:str,num_chunks:int,bucket_id:str ="",timeout:int = 60*2)->Result[GetBytesResponse, Exception]:
        start_time = T.time()
        chunks_ids = ["{}_{}".format(key,i) for i in range(num_chunks)]
        metadatas_futures:List[Awaitable[Result[GetMetadataResponse,Exception]]] = []
        for chunk_id in chunks_ids:
            metadata = self.get_metadata(bucket_id=bucket_id,key= chunk_id)
            metadatas_futures.append(metadata)
        metadatas:List[Metadata]=[]
        current_indexes = []
        for metadata_fut in as_completed(metadatas_futures):
            metadata_result:Result[GetMetadataResponse,Exception] = metadata_fut.result()
            if metadata_result.is_err:
                error = metadata_result.unwrap_err()
                self.__log.error(str(error))
                raise error
            else:
                chunk_metadata = metadata_result.unwrap()
                index = int(chunk_metadata.metadata.tags.get("index","-1"))
                if index == -1:
                    continue
                if not index in current_indexes:
                    current_indexes.append(index)
                    metadatas.append(chunk_metadata.metadata)
        results = []
        for chunk_metadata in metadatas:
            res = self.get(bucket_id=bucket_id,key=chunk_metadata.key,timeout=timeout)
            results.append(res)
        # ______________________________________
        if len(results) == 0:
            return Err(Exception("Key={} not found".format(key)))
        
        xs:List[GetBytesResponse] = []
        for chunk_metadata in as_completed(results):
            result:Result[GetBytesResponse,Exception] = chunk_metadata.result()
            if result.is_ok:
                x:GetBytesResponse = result.unwrap()
                xs.append(x)
        xs = sorted(xs, key=lambda x: int(x.metadata.tags.get("index","-1")))
        merged_bytes = bytearray()
        checksum = ""
        response_time = T.time() - start_time
        size = 0
        tags = {}
        content_type = "application/octet-stream"
        producer_id  = "MictlanX"
        # Mergin tags and get the total size
        for x in xs:
            size += x.metadata.size
            for key1,value in x.metadata.tags.items():
                if not key1  in tags:
                    tags[key1]=[value]
                else:
                    tags[key1].append(value)
            merged_bytes.extend(x.value)
        # _____________________________________________________
        for key2,value in tags.items():
            tags[key2] = J.dumps(value)
            
        chunk_metadata = Metadata(key=key,size=size,checksum=checksum,tags=tags, content_type=content_type, producer_id=producer_id, ball_id=key)
        return Ok(GetBytesResponse(value=merged_bytes, metadata=chunk_metadata,response_time=response_time ))

    def get_and_merge(self, key:str,bucket_id:str="",timeout:int = 60*2)->Awaitable[Result[GetBytesResponse,Exception]]:
        return self.__thread_pool.submit(self.__get_and_merge, key = key,bucket_id=bucket_id,timeout=timeout)
    
    def __get_and_merge(self,key:str,bucket_id:str="",timeout:int = 60*2)->Result[GetBytesResponse, Exception]:
        start_time = T.time()
        _bucket_id = self.__bucket_id if bucket_id == "" else bucket_id
        results:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
        metadatas_gen = self.__get_metadata_valid_index(key=key,timeout=timeout,bucket_id=_bucket_id)
        i = 0 
        worker_buffer_size = int(self.__max_workers/2)
        # print("WORKER_BUFFER_SIZE 1", worker_buffer_size)
        worker_buffer_size = self.__max_workers if worker_buffer_size == 0 else worker_buffer_size
        # print("WORKER_BUFFER_SIZE 2", worker_buffer_size)
        get_reponses:List[GetBytesResponse] = []
        # if len(results) == 0:
        #     return Err(Exception("Key={} not found".format(key)))
        for chunk_metadata in metadatas_gen:
            res   = self.get(bucket_id=_bucket_id,key=chunk_metadata.key,timeout=timeout)
            print(res)
            results.append(res)
            i += 1
            if i % worker_buffer_size == 0:
                for chunk_metadata in as_completed(results):
                    result:Result[GetBytesResponse,Exception] = chunk_metadata.result()
                    if result.is_ok:
                        response:GetBytesResponse = result.unwrap()
                        get_reponses.append(response)
                    else:
                        error = result.unwrap_err()
                        self.__log.error("{}".format(str(error)))
                        raise error
                results.clear()
            
        get_reponses = sorted(get_reponses, key=lambda x: int(x.metadata.tags.get("index","-1")))
        merged_bytes = bytearray()
        checksum = ""
        response_time = T.time() - start_time
        size = 0
        tags = {}
        content_type = "application/octet-stream"
        producer_id = "MictlanX"
        for response in get_reponses:
            size += response.metadata.size
            for key1,value in response.metadata.tags.items():
                if not key1  in tags:
                    tags[key1]=[value]
                else:
                    tags[key1].append(value)
            # 
            merged_bytes.extend(response.value)
        for key2,value in tags.items():
            tags[key2] = J.dumps(value)
            
        chunk_metadata = Metadata(key=key,size=size,checksum=checksum,tags=tags, content_type=content_type, producer_id=producer_id, ball_id=key)
        return Ok(GetBytesResponse(value=merged_bytes, metadata=chunk_metadata,response_time=response_time ))

    


# if __name__ =="__main__":
#     c=  Client(
#         client_id="client-0",
#         peers= [
#             Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=7000),
#             Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=7001),
#         ],
#         debug= True,
#         show_metrics=False,
#         daemon=True,
#         max_workers=10,
#         lb_algorithm="2CHOICES_UF"
#     )
#     x = c.get_bucket_metadata(bucket_id="MICTLANX_GLOBAL_BUCKET")
#     print(x)
    # key = "y"
    # res = c.put(value=b"HOLAA",tags={"KEY":"VALUEE"},key=key).result()
    # print(res)
    # res = c.get_metadata(key=key)
    # print(res.result())
    # ndarray = np.random.randint(0,100,size=(10000,100))
    # key     = "matrix-{}".format(0)
    # chunks  = Chunks.from_ndarray(ndarray=ndarray, group_id=key,num_chunks=3).unwrap()
    # res     = c.put_chunks(key=key,chunks=chunks, tags={"from":"Client test"})
    # for chunk in res:
    #     print(chunk)
    # res = c.get_and_merge_ndarray(key=key)
    # res = c.get_and_merge_with_num_chunks(key=key,num_chunks=3)
    # print("RESULT",res.result())
    # res     = c.put_chunks(key=key,ndarray=ndarray,tags={"tag1":"tag1_value"})
    # print(res)
    # T.sleep(1000)
    # def test()
    # futures:List[Awaitable[Result[GetNDArrayResponse, Exception]]] = []
    # for i in range(10):
        # ndarray = np.random.randint(0,100,size=(10000,100))
        # key     = "matrix-{}".format(i)
        # res     = c.put_ndarray(key=key,ndarray=ndarray,tags={"tag1":"tag1_value"})
        # for i in range(np.random.randint(1,10)):
            # fut = c.get_ndarray(key=key)
            # fut.add_done_callback(lambda x: print("FUTURE_RESULT",i, x.result()))
            # futures.append(fut)
    # T.sleep(60)
            # T.sleep(5)
        # T.sleep(2)
    # for result in as_completed(futures):
        # print("COMPLETED",result.result())
        # if res.is_ok:
            # print("MATRIX_SHAPE",res.unwrap().value.shape)
            # print("-"*20)
        # x= c.get_ndarray(key="matrix-0")
        # print(x)
    #     x= c.get(key="38532d11446c55c07fadc1db2511c9c16146877d491a7472b6203c1ad62fbd0c")
    #     print(x)
        # print("-"*20)
        # T.sleep(.8)
    # with open("/source/01.pdf","rb") as f:
        # res = c.put(key="",value=f.read(),checksum_as_key=True)
    #     print(res)
    # T.sleep(10)
    # _________________________________________-
    # def get(key:str):
        # for i in range(np.random.randint(1,100)):
            # res = c.get_ndarray(key=key)
            # res = c.get_and_merge_ndarray(key=key)
            # print(i,res)
            # T.sleep(.9)
            # T.sleep(np.random.random())
    # with ThreadPoolExecutor(max_workers= 2) as executor:
    # for i in range(1):
        # ndarray = np.random.randint(0,100,size=(10000,100))
        # key     = "matrix-{}".format(i)
        # res = c.put_ndarray(key=key,ndarray=ndarray,tags={"tag1":"tag1_value"})
            # chunks  = Chunks.from_ndarray(ndarray=ndarray,group_id=key,chunk_prefix=Some(key),num_chunks=3).unwrap()
            # chunks = map(lambda chunk: chunk,chunks.iter())
            # res = list(c.put_chunks(key=key,chunks=chunks ,tags={"tag1":"VALUE_1"}))
            # for result in res:
                # print(result)
            # print("_"*15)
            # res = c.get_chunks_metadata(key=key, peer= c.peers[0])
            # res = c.get_and_merge(key=key)
            # res = c.get_and_merge_ndarray(key=key)
            # if res.is_ok:
                # print(res.unwrap().value.shape)
            # print(res)
                # for m in res.unwrap():
                    # print(m)
            # executor.submit(get,key=key)
            # print(res)
            # T.sleep(5)
    # T.sleep(30)
    # c.shutdown()
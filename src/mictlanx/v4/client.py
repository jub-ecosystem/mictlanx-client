
import os
import json as J
import time as T
import requests as R
import numpy as np
import numpy.typing as npt
import humanfriendly as HF
from option import Result,Ok,Err,Option,NONE,Some
from typing import  List,Dict,Generator,Awaitable,Tuple
from mictlanx.v4.interfaces.responses import PutResponse,GetMetadataResponse,GetBytesResponse,GetNDArrayResponse,Metadata,GetBucketMetadataResponse,PutChunkedResponse,PutMetadataResponse,GetRouterBucketMetadataResponse
from mictlanx.logger.log import Log
from threading import Lock
from concurrent.futures import ThreadPoolExecutor,as_completed
from itertools import chain
from functools import reduce
from mictlanx.v4.interfaces.index import PeerStats
from mictlanx.utils.segmentation import Chunks
from mictlanx.v4.xolo.utils import Utils as XoloUtils
from pathlib import Path
from mictlanx.utils.index import Utils,FileInfo
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXHttpHandler,TezcanalyticXParams
from typing_extensions import deprecated
from mictlanx.v4.interfaces.index import Router


API_VERSION = 4 



class Client(object):
    # recommend worker is 4 
    def __init__(
            self,
            client_id:str,
            bucket_id:str = "MICTLANX",
            # peers:List[Router] = [],
            debug:bool=True,
            max_workers:int = 12,
            lb_algorithm:str="ROUND_ROBIN",
            log_output_path:str = "/mictlanx/client",
            # heartbeat_interval:str  = "5s",
            log_when:str="m",
            log_interval:int = 30,
            tezcanalyticx_params:Option[TezcanalyticXParams] = NONE,
            routers:List[Router] = []
            # max_retries:int = 10,
            # tezcanalyticx_log:bool = False
    ):
        # Client unique identifier
        self.client_id = client_id
        # Heartbeats are a abstract way to represents interval time when the client check or calculate in background some metrics. 
        # Every X hearbets the client check the availability of the peers
        # Load balancing algorithm
        self.__lb_algorithm     = lb_algorithm
        # Total number of put operations
        self.__put_counter = 0
        # Total number of get operations
        self.__get_counter = 0
        # Peers
        self.__routers = routers
        self.__lock = Lock()
        self.__bucket_id = bucket_id
        
        # def console_handler_filter(record:L.LogRecord):
        #     if disable_log:
        #         return False
            
        #     if self.__debug and  record.levelno == L.ERROR or record.levelno == L.DEBUG:
        #         return True
        #     elif record.levelno == L.INFO:
        #         return True
        #     else:
        #         return False

        # Log for basic operations
        self.__log         = Log(
            name = self.client_id,
            console_handler_filter =  lambda x: debug,
            # console_handler_filter,
            error_log=True,
            when=log_when,
            interval=log_interval,
            output_path=Some("{}/{}".format(log_output_path,self.client_id))
        )
        if tezcanalyticx_params.is_some:
            x = tezcanalyticx_params.unwrap()
            self.__log.addHandler(
                TezcanalyticXHttpHandler(
                    buffer_size=x.buffer_size,
                    flush_timeout=x.flush_timeout,
                    path=x.path,
                    port=x.port,
                    hostname=x.hostname,
                    protocol=x.protocol,
                    level=x.level
                )
            )
        # 
        
        # get current stats from peers
        # PeerID -> PeerStats
        self.__peer_stats:Dict[str, PeerStats] = {}
        
        # x = retry_call(self.__check_stats_peers, fargs=[self.__peers], tries=max_retries,delay=2,backoff=2)
        # self.__check_stats_peers(self.__peers)

        # self.__put_response_time_dequeue = deque(maxlen=metrics_buffer_size)

        # If the output path does not exists then create it
        if not os.path.exists(log_output_path):
            os.makedirs(name=log_output_path,mode=0o777,exist_ok=True)
        

        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers
        # PeerID -> u64
        self.__put_operations_per_peer= {}
        # PeerID -> u64
        self.__access_total_per_peer= {}
        
        # PeerID.Key -> u64
        # self.__replica_access_counter= {}
        
        # PeerId -> List[Key]
        # self.__chunk_map:Dict[str,List[BallContext]] = {}
        # PeerID -> f32
        # self.disk_ufs:Dict[str, float] = {}
        self.__max_workers = max_workers
        self.__thread_pool = ThreadPoolExecutor(max_workers= self.__max_workers,thread_name_prefix="mictlanx-worker")

    def disable(self,bucket_id:str, key:str,headers:Dict[str,str]={})->List[Result[bool,Exception]]:
        ress = []
        for peer in self.__routers:
            res = peer.disable(bucket_id=bucket_id,key=key,headers=headers)
            ress.append(res)
        return ress
        

        
    def put_file_chunked(self,
                         path:str,
                         chunk_size:str = "1MB",
                         bucket_id:str="",
                         key:str="",
                         checksum_as_key = True,
                         peer_id:Option[str]=NONE,
                         tags:Dict[str,str]={},
                         timeout:int=30,
                         disabled:bool=False,
                         headers:Dict[str,str] = {}
    )->Result[PutChunkedResponse,Exception]:
        try:
            start_time = T.time()
            file_chunks = Utils.file_to_chunks_gen(path=path, chunk_size=chunk_size);
            (checksum,size) = XoloUtils.sha256_file(path=path)
            key     = key if (not checksum_as_key or not key =="")  else checksum
            with self.__lock:
                if peer_id.is_some:
                    _peers = [peer_id.unwrap()]
                else:
                    _peers = list(map(lambda x: x.router_id,self.__routers))
                # ______________________________________________________
                peer    = self.__lb(
                    operation_type = "PUT",
                    algorithm      = self.__lb_algorithm,
                    key            = key,
                    peers          = _peers,
                    size           = size
                )
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            ball_id = key
            put_metadata_result = peer.put_metadata(
                key          = key, 
                size         = size, 
                checksum     = checksum,
                tags         = tags,
                producer_id  = self.client_id,
                content_type = "application/octet-stream",
                ball_id      = ball_id,
                bucket_id    = _bucket_id,
                timeout      = timeout,
                is_disable   = disabled,
                headers= headers
            )
            if put_metadata_result.is_err:
                raise put_metadata_result.unwrap_err()
            put_metadata_response = put_metadata_result.unwrap()
            self.__log.debug({
                "event":"PUT.METADATA.RESPONSE",
                "bucket_id":bucket_id,
                "key":key,
                "peer_id":put_metadata_response.node_id,
                "task_id":put_metadata_response.task_id,
                "service_time":put_metadata_response.service_time
            })

            selected_peer_id = put_metadata_response.node_id
            if put_metadata_response.task_id == "0":
                response_time = T.time() - start_time
                res = PutResponse(
                    key           = key,
                    response_time = response_time,
                    throughput    = float(size) / float(response_time),
                    node_id       = selected_peer_id
                )
                return Ok(res)


            response_time = T.time() - start_time
            # self.__put_response_time_dequeue.append(response_time)
            x = peer.put_chuncked(
                task_id= put_metadata_response.task_id,
                chunks=file_chunks,
                headers={**headers,"Peer-Id":selected_peer_id}
            )
            self.__log.info({
                "event":"PUT.CHUNKED",
                "bucket_id":bucket_id,
                "key":key,
                "size":size,
                "response_time":response_time,
                "peer_id":selected_peer_id
            })
            return x
        except R.exceptions.HTTPError as e:
            # status_code = e.response.status_code 
            self.__log.error({
                "msg":e.response.content.decode("utf-8"),
                # "status_code":status_code 
            })
            return Err(e)
            # return =e.response.status_code, detail=str(e.response.content.decode("utf-8")))
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
        finally:
            with self.__lock:
                self.__put_counter+=1

            
    def get_peer_by_id(self, peer_id:str)->Option[Router]:
        if len(self.__routers) == 0:
            return NONE
        x = next(filter(lambda x: x.router_id == peer_id,self.__routers),self.__routers[0])
        return Some(x)
    # Return the unavailible peers

            

    def __global_operation_counter(self):
        return self.__get_counter + self.__put_counter

    def __lb(self,operation_type:str,algorithm:str="ROUND_ROBIN",key:str="",size:int= 0,peers:List[str]=[])->Router:
        try:
            # filtered_peers = list(filter(lambda x: x.peer_id in peers,self.__peers))
            filtered_peers = self.__routers.copy()
            # print("FILTERED_PEERS",filtered_peers)
            if len(filtered_peers) == 1:
                return filtered_peers[0]
            
            if algorithm =="ROUND_ROBIN":
                return self.__lb_rb(operation_type,peers=filtered_peers)
            elif algorithm == "HASH":
                return self.__lb_hash(operation_type,key=key, peers=filtered_peers)
            elif algorithm == "PSEUDORANDOM":
                return self.__lb_pseudo_random(operation_type,key=key, peers=filtered_peers)
            elif algorithm == "2CHOICES":
                return self.__lb__two_choices(operation_type,key=key, peers=filtered_peers)
            # elif algorithm == "SORT_UF":
            #     return self.__lb_sort_uf(operation_type = operation_type , key= key,size=size, peers=filtered_peers)
            # elif algorithm == "2CHOICES_UF":
            #     return self.__lb_2choices_uf(operation_type = operation_type , key= key,size=size, peers=filtered_peers)
            else:
                return self.__lb_rb(operation_type,peers=filtered_peers)
        except Exception as e:
            self.__log.error("LB_ERROR "+str(e))

    @deprecated("MictlanX - Router has the load balancing algorithms. Now this is useless")
    def __lb_sort_uf(self,operation_type:str,key:str, size:int,peers:List[Router])->Router:
        peers_ids        = list(map(lambda x : x.peer_id ,peers))
        peers_stats      = dict(list(filter(lambda x: x[0] in peers_ids , self.__peer_stats.items())))
        ufs_peers        = dict(list(map(lambda x: (x[0], x[1].calculate_disk_uf(size=size)), peers_stats.items())))
        sorted_ufs_peers = sorted(ufs_peers.items(), key=lambda x: x[1])
        selected_peer_id = sorted_ufs_peers[0][0]
        selected_peer    = next((peer  for peer in peers if peer.peer_id == selected_peer_id),None)
        return selected_peer
        
    @deprecated("MictlanX - Router has the load balancing algorithms. Now this is useless")
    def __lb_2choices_uf(self,operation_type:str,key:str, size:int,peers:List[Router])->Router:
        if operation_type == "PUT":
            peers_ids        = list(map(lambda x : x.peer_id ,peers))
            peers_stats      = dict(list(filter(lambda x: x[0] in peers_ids , self.__peer_stats.items())))
            ufs_peers        = list(map(lambda x: (x[0], x[1].calculate_disk_uf(size=size)), peers_stats.items()))
            peer_x_index     = np.random.randint(low= 0, high=len(ufs_peers))
            peer_y_index     = np.random.randint(low= 0, high=len(ufs_peers))
            max_tries        = len(ufs_peers)
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
            
            if peer_x[1] < peer_y[1]:
                selected_peer    = next((peer  for peer in peers if peer.peer_id == peer_x[0]),None)
                return selected_peer
            else:
                selected_peer    = next((peer  for peer in peers if peer.peer_id == peer_y[0]),None)
                return selected_peer
        else:
            return self.__lb__two_choices(operation_type=operation_type,peers=peers)

        
    def __lb__two_choices(self,operation_type:str,peers:List[Router])-> Router: 
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
            peer_x_put_counter = self.__put_operations_per_peer.get(peer_x.router_id,0)
            peer_y_put_counter = self.__put_operations_per_peer.get(peer_y.router_id,0)
            if peer_x_put_counter < peer_y_put_counter:
                return peer_x
            else:
                return peer_y
        elif operation_type == "GET":
            peer_x_get_counter = self.__access_total_per_peer.get(peer_x.router_id,0)
            peer_y_get_counter = self.__access_total_per_peer.get(peer_y.router_id,0)
            if peer_x_get_counter < peer_y_get_counter:
                return peer_x
            else:
                return peer_y
        else:
            return peer_x



        

        # peers
    def __lb_rb(self,operation_type:str,peers:List[Router]=[])->Router:
        x = self.__global_operation_counter()
        return peers[x % len(peers)]
    
    def __lb_hash(self,key:str, peers:List[Router]=[])->Router:
        return peers[hash(key) % len(peers)]

    def __lb_pseudo_random(self,key:str, peers:List[Router]=[])->Router:
        return peers[np.random.randint(0,len(peers))]


    def shutdown(self):
        try:
            self.__thread_pool.shutdown(wait=False)
            if self.__daemon:
                self.enable_daemon=False
                # self.__thread.join(timeout=30)
            self.__log.debug("SHUTDOWN {}".format(self.client_id))
        except Exception as e:
            print(e)

    def put(self,
            value:bytes,
            tags:Dict[str,str]={},
            checksum_as_key:bool=True,
            key:str="",
            ball_id:str ="",
            bucket_id:str="",
            timeout:int = 60*2,
            disabled:bool=False,
            headers:Dict[str,str]={}
    )-> Awaitable[Result[PutResponse,Exception]]:
        _key = Utils.sanitize_str(x=key)
        _ball_id = Utils.sanitize_str(x=ball_id)
        _bucket_id = Utils.sanitize_str(x=bucket_id)

        if _key == "" and bucket_id =="" :
            return Err(Exception("<key> and <bucket_id> are empty."))

        return self.__thread_pool.submit(self.__put,
                                         key=_key,
                                         value=value,
                                         tags=tags,
                                         checksum_as_key=checksum_as_key,
                                         ball_id = _key if _ball_id == "" else _ball_id,
                                         bucket_id=self.__bucket_id if _bucket_id == "" else _bucket_id,
                                         timeout=timeout,
                                         disabled=disabled,
                                         headers=headers
                )
    def __put(self,
              value:bytes,
              tags:Dict[str,str]={},
              checksum_as_key=True,
              key:str="",
              ball_id:str="",
              bucket_id:str="",
              timeout:int = 60*2,
              disabled:bool = False,
              headers:Dict[str,str]={}
    )->Result[PutResponse,Exception]:
        try:
            # The arrivla time of the put operations
            start_time = T.time()
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

            peer    = self.__lb(
                operation_type = "PUT",
                algorithm      = self.__lb_algorithm,
                key            = key,
                peers          = self.__routers,
                size           = size
            )
     
            content_type = "application/octet-stream"

                
            
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
                is_disable   = disabled,
                headers=headers
            )
            if put_metadata_result.is_err:
                raise put_metadata_result.unwrap_err()
            put_metadata_response:PutMetadataResponse = put_metadata_result.unwrap()

            if put_metadata_response.task_id == "0":
                response_time = T.time() - start_time
                res = PutResponse(
                    key           = key,
                    response_time = response_time,
                    throughput    = float(size) / float(response_time),
                    node_id       = put_metadata_response.node_id
                )
                return Ok(res)

            _headers = {**headers,"Peer-Id":put_metadata_response.node_id}
            put_response = peer.put_data(task_id= put_metadata_response.task_id, key= key, value= value, content_type=content_type,timeout=timeout,headers=_headers)
            if put_response.is_err:
                raise put_response.unwrap_err()
            
            response_time = T.time() - start_time
            # self.__put_response_time_dequeue.append(response_time)
            self.__log.info({
                "event":"PUT",
                "client_id":self.client_id,
                "arrival_time":start_time,
                "bucket_id":bucket_id,
                "ball_id":ball_id,
                "key":key,
                "checksum":checksum,
                "size":size,
                "task_id":put_metadata_response.task_id,
                "metadata_response_time":put_metadata_response.service_time,
                "peer_id":put_metadata_response.node_id,
                "response_time":response_time,
            })

            
            # _____________________________
            res = PutResponse(
                key           = key,
                response_time = response_time,
                throughput    = float(size) / float(response_time),
                node_id       = put_metadata_response.node_id
            )
            return Ok(res)

            
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
    def put_ndarray(self, key:str, ndarray:npt.NDArray,tags:Dict[str,str],bucket_id:str="",timeout:int = 60*2,headers:Dict[str,str]={})->Awaitable[Result[PutResponse,Exception]]:
        try:
            value:bytes = ndarray.tobytes()
            dtype       = str(ndarray.dtype)
            shape_str   = str(ndarray.shape)
            return self.put(key=key, value=value,tags={**tags,"dtype":dtype,"shape":shape_str },bucket_id=bucket_id,timeout=timeout, headers=headers)
        except R.RequestException as e:
            self.__log.error({
                "msg": str(e)
            })
            # if isinstance(e, R.RequestException):
            #     response = e.response
            #     if hasattr(response, "headers"):
            #         headers = response.headers 
            #         error_msg = headers.get("error-message","UKNOWN_ERROR")
            #         self.__log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.__log.error({
              "msg": str(e)
            })
            return Err(e)

    def put_chunks(self,
                   key:str,
                   chunks:Chunks,
                   tags:Dict[str,str],
                   checksum_as_key:bool= False,
                   bucket_id:str="",
                   timeout:int = 60*2,
                #    peers_ids:List[str] = [],
                   update:bool = True,
                   headers:Dict[str,str]={}
    )->Generator[Result[PutResponse,Exception],None,None]:
        
        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        if update:
            self.delete_by_ball_id(ball_id=key,bucket_id=_bucket_id, headers=headers)
        
        futures:List[Awaitable[Result[PutResponse,Exception]]] = []
        # max_peers_ids = len(peers_ids)
        
        for i,chunk in enumerate(chunks.iter()):
            # if max_peers_ids == 0:
            #     peer_id = NONE
            # else:
            #     peer_id = peers_ids[i%max_peers_ids]
            fut = self.put(
                value=chunk.data,
                tags={**tags, **chunk.metadata, "index": str(chunk.index), "checksum":chunk.checksum},
                key=chunk.chunk_id,
                checksum_as_key=checksum_as_key,
                ball_id=key,
                bucket_id=_bucket_id,
                timeout=timeout,
                # peer_id=peer_id
            )
            futures.append(fut)
        
        for result in as_completed(futures):
            res = result.result()
            # if res.is_ok:
            #     chunk_ctx = BallContext(size= chunk.size, locations=[ res.map(lambda x: x.node_id).unwrap() ])
            #     if key in self.__chunk_map:
            #         self.__chunk_map[key].append(chunk_ctx)
            #     else:
            #         self.__chunk_map[key] =[chunk_ctx]
            
            yield res

    # GET
    
    def __get_bucket_metadata(self,bucket_id:str, router:Router=NONE, timeout:int = 60*2,headers:Dict[str,str]={})->Result[GetRouterBucketMetadataResponse,Exception]: 
        try:
            start_time = T.time()
            x = router.get_bucket_metadata(bucket_id=bucket_id,timeout=timeout,headers=headers)
            if x.is_ok:
                service_time = T.time() - start_time
                self.__log.info({
                    "event":"GET_BUCKET_METADATA",
                    "bucket_id":bucket_id,
                    "router_id":router.router_id,
                    "service_time":service_time
                })
                return x
            else:
                service_time = T.time() - start_time
                self.__log.error({
                    "msg":str(x.unwrap_err()),
                    "bucket_id":bucket_id,
                    "router_id":router.router_id,
                    "service_time":service_time
                })
                return x
                
        except Exception as e:
            return Err(e)

    def get_bucket_metadata(self,bucket_id:str, router:Option[Router]=NONE, timeout:int = 60*2,headers:Dict[str,str]={})->Awaitable[Result[GetRouterBucketMetadataResponse, Exception]]:
        start_time = T.time()
        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        try:
            if router.is_none:
                _router = self.__lb(
                    operation_type = "GET",
                    algorithm      = "ROUND_ROBIN",
                    key            = _bucket_id,
                    peers          = list(map(lambda x: x.router_id,self.__routers))
                )
            else:
                _router = router.unwrap()
            x     = self.__thread_pool.submit(self.__get_bucket_metadata, bucket_id=_bucket_id,timeout=timeout, router= _router,headers=headers)
            # service_time = T.time() - start_time
            return x
        except Exception as e:
            return Err(e)


    def get_ndarray(self, key:str,bucket_id:str="",timeout:int= 60*2,headers:Dict[str,str]={})->Awaitable[Result[GetNDArrayResponse,Exception]]:
        start_time = T.time()
        try:
            
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            def __inner()->Result[GetNDArrayResponse,Exception]:
                get_result =  self.__get(key=key,timeout=timeout,bucket_id=_bucket_id,headers=headers)
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

 
    def get_metadata(self,key:str,bucket_id:str="",peer:Option[Router]=NONE ,timeout:int=60*2,headers:Dict[str,str]={}) -> Awaitable[Result[GetMetadataResponse, Exception]] :
        if peer.is_none:
            _peer = self.__lb(
                    operation_type="GET",
                    algorithm=self.__lb_algorithm,
                    key=key,
                    size=0,
                    peers=list(map(lambda x: x.router_id,self.__routers))
            )
        else:
            _peer = peer.unwrap()
        
        # _peer.get_metadata()
        _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
        return self.__thread_pool.submit(_peer.get_metadata, key = key, timeout = timeout,bucket_id=_bucket_id,headers=headers)

    def get_to_file(self,
                    key:str,
                    bucket_id:str,
                    filename:str="",
                    chunk_size:str="1MB",
                    output_path:str="/mictlanx/data",
                    timeout:int = 60*2,
                    # peer_id:Option[str]=NONE,
                    headers:Dict[str,str]={}
    )->Result[str,Exception]:
        start_time = T.time()
        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
            
        # if peer_id.is_some:
        #     selected_peer = self.__lb(operation_type="GET", algorithm=self.__lb_algorithm, key=key, size=0,  peers=[peer_id.unwrap()])
        # else:
        routers_ids = list(map(lambda x:x.router_id, self.__routers))
        selected_peer = self.__lb(operation_type="GET", algorithm=self.__lb_algorithm, key=key, size=0,  peers=routers_ids )
        result = selected_peer.get_to_file(bucket_id=_bucket_id,key=_key,chunk_size=chunk_size,sink_folder_path=output_path,filename=filename,timeout=timeout,headers=headers)
        return result
            

    def get (self,key:str,bucket_id:str="",timeout:int = 60*2,headers:Dict[str,str]={},chunk_size:str="1MB")->Awaitable[Result[GetBytesResponse,Exception]]:

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id

        if _key == "" and bucket_id =="" :
            return Err(Exception("<key> and <bucket_id> are empty."))
        x = self.__thread_pool.submit(
            self.__get, key = _key,timeout=timeout,bucket_id=_bucket_id,headers=headers,chunk_size = chunk_size
        )
        return x

    def __get(self,key:str,bucket_id:str="",timeout:int=60*2,chunk_size:str="1MB",headers:Dict[str,str]={})->Result[GetBytesResponse,Exception]:
        try:
            _chunk_size= HF.parse_size(chunk_size)
            start_time = T.time()
            
            bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            selected_peer = self.__lb(
                operation_type = "GET",
                algorithm      = self.__lb_algorithm,
                key            = key,
                size           = 0,
                peers          = list(map(lambda x: x.router_id,self.__routers))
            )
            # _____________________________________________________________________________________________________________________
            get_metadata_result:Result[GetMetadataResponse,Exception] = selected_peer.get_metadata(bucket_id=bucket_id,key=key,timeout=timeout, headers=headers)
            # self.__get_metadata(bucket_id=bucket_id,key= key, peer=selected_peer,timeout=timeout)
            # .result()
            if get_metadata_result.is_err:
                raise Exception(str(get_metadata_result.unwrap_err()))
            metadata_response = get_metadata_result.unwrap()
            metadata_service_time = T.time() - start_time
            # _________________________________________________________________________
            result = selected_peer.get_streaming(bucket_id=bucket_id,key=key,timeout=timeout,headers=headers)
            if result.is_err:
                raise result.unwrap_err()

            response = result.unwrap()
            response_time = T.time() - start_time
            value = bytearray()
            for chunk in response.iter_content(chunk_size=_chunk_size):
                # print("CHUNK",len(chunk))
                value.extend(chunk)
            # 
            self.__log.info(
                {
                    "event":"GET",
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":metadata_response.metadata.size, 
                    "response_time":response_time,
                    "metadata_service_time":metadata_service_time,
                    "peer_id":metadata_response.node_id,
                }
            )
            return Ok(GetBytesResponse(value=bytes(value),metadata=metadata_response.metadata,response_time=response_time))

        except R.RequestException as e:
            self.__log.error(str(e))
            return Err(e)
        except Exception as e:
            self.__log.error(str(e))
            return Err(e)


    def get_and_merge_ndarray(self,key:str,bucket_id:str="",timeout:int= 60*2,headers:Dict[str,str]={}) -> Awaitable[Result[GetNDArrayResponse,Exception]]:

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        return self.__thread_pool.submit(self.__get_and_merge_ndarray, key = _key,timeout=timeout,bucket_id=_bucket_id,headers=headers)
    
    def __get_and_merge_ndarray(self,key:str,bucket_id:str="",timeout:int=60*2,headers:Dict[str,str]={})->Result[GetNDArrayResponse,Exception] :
        start_time = T.time()
        res:Result[GetBytesResponse,Exception] = self.__get_and_merge(bucket_id=bucket_id,key=key,timeout=timeout,headers=headers)
        if res.is_ok:
            response  = res.unwrap()
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
    
    def __get_metadata_peers_async(self,key:str,bucket_id:str="",timeout:int = 60*2,headers:Dict[str,str]={})->Generator[List[Metadata],None,None]:
        for peer in self.__routers:
            metadata_result = peer.get_chunks_metadata(key=key,bucket_id=bucket_id,timeout=timeout,headers=headers)
            if metadata_result.is_ok:
                yield metadata_result.unwrap()



    def __get_metadata_valid_index(self,key:str,bucket_id:str="",timeout:int = 60*2,headers:Dict[str,str]={})->Generator[Metadata,None,None]: 
        # ______________________________________________________________

        metadatas  = list(self.__get_metadata_peers_async(bucket_id=bucket_id,key=key,timeout=timeout,headers=headers))
        if len(metadatas) >0:
            reduced_metadatas = reduce(lambda a,b: chain(a,b) ,metadatas)
        else:
            return  None
        
        current_indexes   = []
        # ______________________________________________________________
        for chunk_metadata in reduced_metadatas:
            index = int(chunk_metadata.tags.get("index","-1"))
            if index == -1:
                continue
            if not index in current_indexes:
                current_indexes.append(index)
                yield chunk_metadata

    def get_and_merge_with_num_chunks(self, key:str,num_chunks:int,bucket_id:str = "",timeout:int = 60*2, max_retries:Option[int]=NONE,headers:Dict[str,str]={})->Awaitable[Result[GetBytesResponse,Exception]]:

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id

        return self.__thread_pool.submit(self.__get_and_merge_with_num_chunks,
                                          key = _key,
                                          timeout=timeout,
                                          num_chunks= num_chunks,
                                          bucket_id = _bucket_id,
                                          max_retries=max_retries,
                                          headers= headers
        )
    

    def __get_chunks_and_fails(self,bucket_id:str,chunks_ids:List[str],timeout:int=60*2,headers:Dict[str,str]={})->Tuple[List[GetBytesResponse], List[str]]:
        max_iter = len(self.__routers)*2
        xs:List[GetBytesResponse] = []
        failed_chunk_keys = []
        for chunk_key in chunks_ids:
            i = 0 
            res:Result[GetBytesResponse,Exception] = self.get(bucket_id=bucket_id,key=chunk_key,timeout=timeout,headers=headers).result()
            # ______________________________________
            while res.is_err and i < max_iter:
                res:Result[GetBytesResponse,Exception] = self.get(bucket_id=bucket_id,key=chunk_key,timeout=timeout).result()
                i+=1
                if i>=max_iter and res.is_err:
                    failed_chunk_keys.append(chunk_key)
            if res.is_ok:
                x = res.unwrap()
                xs.append(x)
        return (xs, failed_chunk_keys)
    
    
    def __get_and_merge_with_num_chunks(self,key:str,num_chunks:int,bucket_id:str ="",timeout:int = 60*2,max_retries:Option[int]=NONE,headers:Dict[str,str]={})->Result[GetBytesResponse, Exception]:
        start_time     = T.time()
        _chunk_indexes = list(range(num_chunks))
        chunks_ids     = ["{}_{}".format(key,i) for i in _chunk_indexes]
        (xs, fails)    = self.__get_chunks_and_fails(bucket_id=bucket_id,chunks_ids=chunks_ids,timeout=timeout,headers=headers)
        service_time   = T.time() -start_time

        self.__log.info({
            "bucket_id":bucket_id,
            "key":key,
            "fails":len(fails),
            "num_chunks":num_chunks,
            "service_time":service_time,
            "retry_counter":0
        })
        xss =[*xs]
        i=0
        if max_retries.is_none:
            max_iter = len(fails) * 2
        else:
            max_iter = max_retries.unwrap_or(len(fails)*2)
        

        while len(fails) > 0 and i < max_iter:
            (xs, fails) = self.__get_chunks_and_fails(bucket_id=bucket_id,chunks_ids=fails,timeout=timeout)
            if len(xs)>0:
                xss=[*xss, *xs]
            i+=1
            self.__log.info({
                "bucket_id":bucket_id,
                "key":key,
                "fails":len(fails),
                "num_chunks":num_chunks,
                "service_time":service_time,
                "max_retries":max_iter,
                "retry_counter":i
            })

        if len(xss) != num_chunks:
            return Err(Exception("Fail to get {}".format(fails)))
        xss = sorted(xss, key=lambda x: int(x.metadata.tags.get("index","-1")))
        merged_bytes = bytearray()
        checksum = ""
        response_time = T.time() - start_time
        size = 0
        tags = {}
        content_type = "application/octet-stream"
        producer_id  = "MictlanX"
        # Mergin tags and get the total size
        for x in xss:
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

    def get_and_merge(self, key:str,bucket_id:str="",timeout:int = 60*2,headers:Dict[str,str]={})->Awaitable[Result[GetBytesResponse,Exception]]:

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        return self.__thread_pool.submit(self.__get_and_merge,
                                         key = _key,
                                         bucket_id=_bucket_id,
                                         timeout=timeout,
                                         headers = headers
        )
    
    def __get_and_merge(self,key:str,bucket_id:str="",timeout:int = 60*2,headers:Dict[str,str]={})->Result[GetBytesResponse, Exception]:
        start_time = T.time()
        _bucket_id = self.__bucket_id if bucket_id == "" else bucket_id
        results:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
        metadatas_gen = list(self.__get_metadata_valid_index(key=key,timeout=timeout,bucket_id=_bucket_id,headers=headers))
        if len(metadatas_gen)==0:
            return Err(Exception("{}/{} not found".format(bucket_id,key)))
        i = 0 
        worker_buffer_size = int(self.__max_workers/2)
        worker_buffer_size = self.__max_workers if worker_buffer_size == 0 else worker_buffer_size
        get_reponses:List[GetBytesResponse] = []
        for chunk_metadata in metadatas_gen:
            res   = self.get(bucket_id=_bucket_id,key=chunk_metadata.key,timeout=timeout,headers=headers)
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
                        return Err(error)
                        # raise error
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

    def delete_by_ball_id(self,ball_id:str,bucket_id:str="",timeout:int=60*2,headers:Dict[str,str]={})->Result[None,Exception]:
        _bucket_id = self.__bucket_id if bucket_id == "" else bucket_id
        try:
            for peer in self.__routers:
                start_time = T.time()
                chunks_metadata_result = peer.get_chunks_metadata(key=ball_id,bucket_id=_bucket_id,timeout=timeout,headers=headers)
                if chunks_metadata_result.is_ok:
                    response = chunks_metadata_result.unwrap()
                    for chunk_metadata in response:
                        del_result = peer.delete(bucket_id=_bucket_id, key=chunk_metadata.key)
                        service_time = T.time() - start_time
                        self.__log.debug({
                           "bucket_id":_bucket_id,
                           "ball_id":ball_id,
                           "service_time":service_time
                        })
                        
            # self.__chunk_map[ball_id]=[]
            return Ok(None)
        except Exception as e:
            return Err(e)
    

    def delete(self, key:str,bucket_id:str="",timeout:int = 60*2,headers:Dict[str,str]={})->Result[str, Exception]:

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        try:
            failed=[]
            for router in self.__routers:
                start_time = T.time()
                x = router.delete(bucket_id=_bucket_id,key=_key,headers=headers,timeout=timeout)
                if x.is_err:
                    service_time = T.time() - start_time
                    self.__log.error({
                        "bucket_id":_bucket_id,
                        "key":key,
                        "router_id":router.router_id,
                        "event":"DELETE",
                        "service_time":service_time
                    })
                    failed.append(router)
                else:
                    service_time = T.time() - start_time
                    self.__log.info({
                        "bucket_id":_bucket_id,
                        "key":key,
                        "router_id":router.router_id,
                        # "peer_id":peer.peer_id,
                        "event":"DELETE",
                        "service_time":service_time
                    })
            return Ok(_key)
        except Exception as e:
            return Err(e)
    
    def put_file(self,path:str, bucket_id:str= "", update= True, timeout:int = 60*2, source_folder:str= "",tags={},headers:Dict[str,str]={})->Result[PutResponse,Exception]:
        _bucket_id = self.__bucket_id if bucket_id=="" else bucket_id
        try:
            if not os.path.exists(path=path):
                return Err(Exception("{} not found".format(path)))
            else:
                with open(path,"rb") as f:
                    value = f.read()
                    (checksum,_) = XoloUtils.sha256_file(path=path)
                    if update:
                        self.delete(bucket_id=_bucket_id, key=checksum,timeout=timeout,headers=headers)
                    
                    bucket_relative_path = path.replace(source_folder,"")
                    x = bucket_relative_path.split("/")[-1].split(".")
                    if len(x) == 2:
                        filename,ext  = x
                    else:
                        filename = x[0]
                        ext = ""
                    
                    return self.__put(
                        value     = value,
                        bucket_id = _bucket_id,
                        key       = checksum,
                        tags      = {
                            **tags,
                            "full_path":path,
                            "bucket_relative_path":bucket_relative_path,
                            "filename":filename,
                            "extension":ext, 
                        },
                        headers=headers
                    )
        except Exception as e:
            return Err(e)
    


    
    def put_folder_async(self,source_path:str,bucket_id="",update:bool = True,headers:Dict[str,str]={}) -> Generator[PutResponse, None,None]:
        # _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        if not os.path.exists(source_path):
            return Err(Exception("{} does not exists".format(source_path)))
        
        failed_operations = []
        _start_time = T.time()
        files_counter = 0
        futures:List[Awaitable[Result[PutResponse,Exception]]] = []

        for (root,_, filenames) in os.walk(source_path):
            for filename in filenames:
                start_time = T.time()
                path = "{}/{}".format(root,filename)
                
                # put_file_result = self.put_file(
                #     path=path,
                #     bucket_id=_bucket_id,
                #     update=update,
                #     source_folder=source_path
                # )
                put_file_future = self.__thread_pool.submit(self.put_file, 
                    path=path,
                    bucket_id=_bucket_id,
                    update=update,
                    source_folder=source_path,
                    headers=headers
                )
                futures.append(put_file_future)
          
                    
        

        for fut in as_completed(futures):
            put_file_result:Result[PutResponse,Exception] = fut.result()
            if put_file_result.is_err:
                self.__log.error({
                    "event":"PUT_FILE_ERROR",
                    "error":str(put_file_result.unwrap_err()),
                    "bucket_id":bucket_id,
                    "path": path,
                    "folder_path":source_path
                })
                failed_operations.append(path)
            else:
                response = put_file_result.unwrap()
                service_time = T.time() - start_time
                self.__log.info({
                    "event":"PUT_FILE",
                    "bucket_id":_bucket_id,
                    "key":response.key,
                    "peer_id":response.node_id, 
                    # "foler_p"
                    "path":path,
                    "service_time":service_time
                })
                files_counter+=1
                yield response
            

        service_time = T.time() - _start_time
        self.__log.info({
            "event":"PUT_FOLDER_ASYNC",
            "bucket_id":bucket_id,
            "folder_path":source_path,
            "response_time":service_time,
            "files_counter":files_counter,
            "failed_puts":len(failed_operations),
        })


    def put_folder(self,source_path:str,bucket_id="",update:bool = True,headers:Dict[str,str]={}) -> Generator[PutResponse, None,None]:
        _bucket_id = self.__bucket_id if bucket_id=="" else bucket_id
        if not os.path.exists(source_path):
            return Err(Exception("{} does not exists".format(source_path)))
        
        failed_operations = []
        _start_time = T.time()
        files_counter = 0
        for (root,folders, filenames) in os.walk(source_path):
            for filename in filenames:
                start_time = T.time()
                path = "{}/{}".format(root,filename)
                
                put_file_result = self.put_file(
                    path=path,
                    bucket_id=_bucket_id,
                    update=update,
                    source_folder=source_path
                )
                if put_file_result.is_err:
                    self.__log.error({
                        "event":"PUT_FILE",
                        "error":str(put_file_result.unwrap_err()),
                        "bucket_id":bucket_id,
                        "path": path,
                        "folder_path":source_path
                    })
                    failed_operations.append(path)
                else:
                    response = put_file_result.unwrap()
                    service_time = T.time() - start_time
                    self.__log.info({
                        "event":"PUT_FILE",
                        "bucket_id":_bucket_id,
                        "key":response.key,
                        "peer_id":response.node_id, 
                        "foler_p"
                        "path":path,
                        "service_time":service_time
                    })
                    files_counter+=1
                    yield response
                    
        service_time = T.time() - _start_time
        self.__log.info({
            "event":"PUT_FOLDER",
            "bucket_id":bucket_id,
            "folder_path":source_path,
            "response_time":service_time,
            "files_counter":files_counter,
            "failed_puts":len(failed_operations),
        })
            # print(root,folders,filenames)


    

    # def __get_all_bucket_data_v2_success(self,bucket_id:str,key:str,local_path:Path,chunk_size:str="1MB",fullname:str="",headers:Dict[str,str]={}):
    #     start_time = T.time()
    #     # peer = self.__lb(operation_type="GET",algorithm=self.__lb_algorithm, key=key, peers=[peer_id ])
        
    #     local_path_parent_str = str(local_path.parent)

    #     os.makedirs(local_path_parent_str,exist_ok=True)
    #     # self.__log.debug({
    #     #     "event":"GET.TO.FILE",
    #     #     "bucket_id":bucket_id,
    #     #     "key":key,
    #     #     "local_path":local_path_parent_str,
    #     #     "full_path":bucket_relative_path,
    #     #     "peer_id":peer_id
    #     # })
    #     get_to_file_result = self.get_to_file(
    #         bucket_id=bucket_id,
    #         key=key,
    #         chunk_size=chunk_size,
    #         sink_folder_path=local_path_parent_str,
    #         filename= fullname,
    #         headers=headers
    #     )
    #     if get_to_file_result.is_err:
    #         failed_get_counter+=1
    #         response_time = T.time() - start_time
    #         self.__log.error({
    #             "event":"GET.ERROR",
    #             "error":str(get_to_file_result.unwrap_err()),
    #             "bucket_id":bucket_id,
    #             "response_time":response_time
    #         })                    
    #     else:
    #         response_time = T.time() - start_time
    #         completed_get_counter+=1
    #         self.__log.info({
    #             "event":"GET.TO.FILE",
    #             "bucket_id":bucket_id,
    #             "key":key,
    #             "local_path":str(local_path),
    #             "response_time": response_time
    #         })
            
    #         # local_checksums_list.append(metadata.checksum)
    # def get_all_bucket_datav2(self,
    #                           bucket_id:str,
    #                           skip_files:List[str]=[],
    #                           output_folder:str="/mictlanx/out",
    #                           all:bool=True,
    #                           bucket_folder_as_root:bool=True,
    #                           chunk_size:str="1MB",
    #                           headers:Dict[str,str]={}
    # ) -> Generator[Metadata, None,None]:
    #     try:
    #         if bucket_folder_as_root:
    #             base_path = Path(output_folder,bucket_id)
    #         else:
    #             base_path = Path(output_folder)
    #         if not base_path.exists():
    #             os.makedirs(base_path)
            
    #         global_start_time               = T.time()
    #         bucket_metadatas                = self.get_all_bucket_metadata(bucket_id=bucket_id,headers=headers)
    #         completed_get_counter:int       = 0
    #         failed_get_counter:int          = 0
    #         current_fs_content:List[FileInfo] = list(map(lambda x: x.upadate_path_relative_to(relative_to=str(base_path)),Utils.get_checksums_and_sizes(path=str(base_path))))
    #         local_checksums_list:List[str] = list(map(lambda x: x.checksum,current_fs_content))
    #         # key -> (updated_at, Metadata,FileInfo)
    #         redundant_files:Dict[str, List[Tuple[int,Metadata,FileInfo,str]]] = {}

    #         for bucket_metadata in bucket_metadatas:
    #             current_peer_id = bucket_metadata.peer_id

    #             for metadata in bucket_metadata.balls:
    #                 # SKIP 
    #                 start_time            = T.time()
    #                 if metadata.key in skip_files:
    #                     self.__log.debug({
    #                         "event":"SKIPPED",
    #                         "bucket_id":bucket_id,
    #                         "key":metadata.key,
    #                     })
    #                     continue
                    


    #                 tags                  = metadata.tags
    #                 bucket_relative_path  = tags.get("bucket_relative_path",-1)
    #                 updated_at            = int(tags.get("updated_at",-1))
    #                 file_info             = FileInfo(bucket_relative_path,metadata.checksum, metadata.size)
    #                 # ______________________________________________________________
    #                 if bucket_relative_path == -1:
    #                     self.__log.error({
    #                         "event":"NO_BUCKET_RELATIVE_PATH",
    #                         "bucket_id":bucket_id,
    #                         "key":metadata.key,
    #                         "skip":1
    #                     })
                        
    #                     if not all:
    #                         continue
    #                     else:
    #                         local_path  = base_path / metadata.key
    #                 else:
    #                     local_path =base_path /  Path(bucket_relative_path.lstrip("/"))
    #                 # __________________CHECKSUM IN LOCAL FS_______________________________
    #                 if metadata.checksum in local_checksums_list:
    #                     self.__log.debug({
    #                         "event":"HIT.LOCAL.FILE",
    #                         "bucket_id":bucket_id,
    #                         "key":metadata.key,
    #                         "bucket_relative_path":bucket_relative_path,
    #                         "updated_at":updated_at
    #                     })
    #                     current_redundant_files = redundant_files.setdefault(metadata.key,[])
    #                     current_redundant_files_len = len(current_redundant_files)
    #                     if  current_redundant_files_len >1:
    #                         self.__log.debug({
    #                             "event":"FILE.INCONSISTENCTY.FOUND",
    #                             "bucket_id":bucket_id,
    #                             "key":metadata.key,
    #                             "current_redundant_versions":current_redundant_files_len
    #                         })
                            
    #                     redundant_files[metadata.key].append((updated_at,metadata,file_info,current_peer_id))
    #                     continue
    #                 else:
                    
    #                     if local_path.exists() :
    #                         (local_checksum, size) = XoloUtils.sha256_file(str(local_path))
    #                         if metadata.checksum == local_checksum:
    #                             self.__log.info({
    #                                 "event":"HIT.FILE",
    #                                 "buket_id":bucket_id,
    #                                 "key":metadata.key,
    #                                 "size":metadata.size,
    #                                 "local_checksum":local_checksum,
    #                                 "checksum": metadata.checksum,
    #                                 "size":size
    #                             })
    #                             continue
    #                     else:
    #                         self.__get_all_bucket_data_v2_success(
    #                             bucket_id=bucket_id,
    #                             key=metadata.key,
    #                             # peer_id=current_peer_id,
    #                             local_path=local_path,
    #                             # bucket_relative_path=bucket_relative_path,
    #                             chunk_size=chunk_size,
    #                             fullname=tags.get("fullname",""),
    #                             headers= {"Peer-Id":current_peer_id}
    #                         )

    #                         redundant_files.setdefault(metadata.key,[])
    #                         redundant_files[metadata.key].append((updated_at,metadata,file_info,current_peer_id))
    #                         local_checksums_list.append(metadata.checksum)
    #                         yield metadata

    #             global_response_time = T.time() - global_start_time
    #             self.__log.info({
    #                 "event":"GET_ALL_BUCKET_DATA",
    #                 "completed_gets":completed_get_counter,
    #                 "failed_gets": failed_get_counter,
    #                 "total_gets":completed_get_counter+failed_get_counter, 
    #                 "response_time":global_response_time,
    #                 "output_folder":output_folder
    #             })
            
    #         redundant_fle_infos = list(chain.from_iterable(list(map(lambda x : list(map(lambda y: y[2],x)),redundant_files.values()))))
    #         to_remove_list      = set(current_fs_content).difference( set(redundant_fle_infos  ))
    #         for fi in to_remove_list:
    #             path = "{}/{}".format(str(base_path),fi.path)
    #             if os.path.exists(path):
    #                 os.remove(path)

    #         if len(redundant_files) >0:
    #             self.__log.debug({
    #                 "event":"RESOLVE.CONSISTENCY",
    #                 "redundant_files":len(redundant_files)
    #             })
    #             for (_, redundants) in redundant_files.items():
    #                 if len(redundants) > 1:
    #                     (updated_at,metadata,file_info,peer_id) = max(redundants, key=lambda x: x[0])

    #                     tags                  = metadata.tags
    #                     bucket_relative_path  = tags.get("bucket_relative_path",-1)
    #                     local_path =base_path /  Path(bucket_relative_path.lstrip("/"))
    #                     self.__get_all_bucket_data_v2_success(
    #                         bucket_id=bucket_id,
    #                         key= metadata.key,
    #                         # peer_id=[peer_id],
    #                         local_path= local_path, 
    #                         bucket_relative_path=str(file_info.path),
    #                         chunk_size=chunk_size,
    #                         fullname =metadata.tags.get("fullname",""),
    #                         headers={"Peer-Id":peer_id}
    #                     )
    #                     yield metadata
    #         return []
    #     except Exception as e:
    #         self.__log.error({
    #             "msg":str(e)
    #         })
    #         return Err(e)
    
    
    
    # def get_all_bucket_data(self,
    #                         bucket_id:str, 
    #                         skip_files:List[str]=[],
    #                         output_folder:str="/mictlanx/out",
    #                         duplicates:bool= True,
    #                         all:bool=True,
    #                         bucket_folder_as_root:bool=True,
    #                         headers:Dict[str,str]={}
    # ) -> Generator[Metadata, None,None]:
    #     try:
    #         if bucket_folder_as_root:
    #             base_path = Path(output_folder,bucket_id)
    #         else:
    #             base_path = Path(output_folder)
    #         if not base_path.exists():
    #             os.makedirs(base_path)
            
    #         global_start_time               = T.time()
    #         bucket_metadatas                = self.get_all_bucket_metadata(bucket_id=bucket_id)
    #         completed_balls:List[str]       = []
    #         data_local_paths:Dict[str,Path] = {}
    #         completed_get_counter:int       = 0
    #         failed_get_counter:int          = 0
    #         # Get track of the update_at
    #         consistency_map:Dict[str, int] = {}

    #         for bucket_metadata in bucket_metadatas:
    #             futures:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
    #             current_peer_id = bucket_metadata.peer_id

    #             for ball in bucket_metadata.balls:
    #                 tags                  = ball.tags
    #                 bucket_relative_path  = tags.get("bucket_relative_path",-1)
    #                 updated_at            = tags.get("updated_at",-1)
    #                 is_in_consistency_map = ball.key in consistency_map
    #                 is_the_most_recent    = False
    #                 if is_in_consistency_map and updated_at != -1 :
    #                     if updated_at > consistency_map.get(ball.key,-1):
    #                         consistency_map[ball.key] = updated_at
    #                         is_the_most_recent = True
                    
    #                 if ball.key in completed_balls or ball.key in skip_files:
    #                     continue
    #                 # consis



    #                 if bucket_relative_path == -1:
    #                     self.__log.error({
    #                         "event":"NO_BUCKET_RELATIVE_PATH",
    #                         "bucket_id":bucket_id,
    #                         "key":ball.key,
    #                         "skip":1
    #                     })
                        
    #                     if not all:
    #                         continue
    #                     else:
    #                         local_path  = base_path / ball.key
    #                 else:
    #                     local_path =base_path /  Path(bucket_relative_path.lstrip("/"))
                    
    #                 if local_path.exists() :
    #                     (local_checksum, size) = XoloUtils.sha256_file(str(local_path))
    #                     if ball.checksum == local_checksum:
    #                         self.__log.info({
    #                             "event":"HIT_FILE",
    #                             "buket_id":bucket_id,
    #                             "key":ball.key,
    #                             "size":ball.size,
    #                             "local_checksum":local_checksum,
    #                             "checksum": ball.checksum,
    #                             "size":size
    #                         })
    #                         continue
    #                     else:
    #                         if duplicates:
    #                             version_id = uuid4().hex[:8]
    #                             _local_path =base_path /  Path(bucket_relative_path.lstrip("/")).parent / "{}_{}".format(ball.key,version_id)
    #                             self.__log.info({
    #                                 "event":"DUPLICATE",
    #                                 "duplicates":1,
    #                                 "bucket_id":bucket_id,
    #                                 "key":ball.key,
    #                                 "version_id":version_id,
    #                                 "local_path":str(_local_path),
    #                                 "full_path":str(bucket_relative_path),
    #                             })
    #                             os.makedirs(_local_path.parent,exist_ok=True)
    #                             data_local_paths[ball.key] = _local_path
    #                             futures.append(self.get(key=ball.key,bucket_id=bucket_id))
    #                 else:
                        
    #                     # if not is_in_consistency_map:
    #                     os.makedirs(local_path.parent,exist_ok=True)
    #                     data_local_paths[ball.key] = local_path
    #                     self.__log.info({
    #                         "event":"GET_FROM_BUCKET",
    #                         "bucket_id":bucket_id,
    #                         "key":ball.key,
    #                         "local_path":str(local_path),
    #                         "full_path":str(bucket_relative_path),
    #                     })
    #                     futures.append(self.get(key=ball.key,bucket_id=bucket_id))
    #                     # elif is_in_consistency_map and is_the_most_recent:
    #                         # if 
                
    #             for future in as_completed(futures):
    #                 start_time = T.time()
    #                 result:Result[GetBytesResponse,Exception] = future.result()
    #                 if result.is_ok:
    #                     response = result.unwrap()

    #                     completed_balls.append(response.metadata.key)
    #                     local_path = data_local_paths.get(response.metadata.key)
    #                     with open(local_path,"wb") as f:
    #                         f.write(response.value) 
    #                     response_time = T.time() - start_time
    #                     completed_get_counter+=1
    #                     self.__log.info({
    #                         "event":"GET_COMPLETED",
    #                         "bucket_id":bucket_id,
    #                         "key":response.metadata.key,
    #                         "size":response.metadata.size,
    #                         "local_path":str(local_path),
    #                         "response_time":response_time
    #                     })
    #                     response.metadata.tags["local_path"] = str(local_path)
    #                     yield response.metadata
    #                 else:
    #                     failed_get_counter+=1
    #                     response_time = T.time() - start_time
    #                     self.__log.error({
    #                         "event":"GET_ERROR",
    #                         "error":str(result.unwrap_err()),
    #                         "bucket_id":bucket_id,
    #                         "response_time":response_time
    #                     })                    
    #         global_response_time = T.time() - global_start_time
    #         self.__log.info({
    #             "event":"GET_ALL_BUCKET_DATA",
    #             "completed_gets":completed_get_counter,
    #             "failed_gets": failed_get_counter,
    #             "total_gets":completed_get_counter+failed_get_counter, 
    #             "response_time":global_response_time,
    #             "output_folder":output_folder
    #         })
    #     except Exception as e:
    #         self.__log.error(str(e))
    #         return Err(e)
    


    def get_all_bucket_metadata(self, bucket_id:str,headers:Dict[str,str ]={})-> Generator[GetRouterBucketMetadataResponse,None,None]:
        futures = []
        start_time = T.time()
        for peer in self.__routers:
            fut = self.get_bucket_metadata(bucket_id=bucket_id,router= Some(peer),headers=headers)
            futures.append(fut)
        for fut in as_completed(futures):
            bucket_metadata_result:Result[GetRouterBucketMetadataResponse,Exception] = fut.result()
            if bucket_metadata_result.is_err:
                self.__log.error({
                    "bucket_id":bucket_id,
                    "event":"GET_ALL_BUCKET_METADATA",
                    "error": str(bucket_metadata_result.unwrap_err())
                })
            else:
                bucket_metadata = bucket_metadata_result.unwrap()
                service_time = T.time() - start_time
                self.__log.info({
                    "event":"GET_ALL_BUCKET_METADATA",
                    "bucker_id":bucket_id,
                    "total_files": len(bucket_metadata.balls),
                    "peers_ids":bucket_metadata.peer_ids,
                    "service_time": service_time
                })
                yield bucket_metadata
        


if __name__ =="__main__":
    peers = Utils.peers_from_str_v2(peers_str="router-0:localhost:60666")
    bucket_id = "b3"
    client = Client(
        client_id    = os.environ.get("CLIENT_ID","client-0"),
        # 
        peers        = list(peers),
        # 
        debug        = True,
        # 
        daemon       = False, 
        show_metrics = False,
        # 
        max_workers  = 2,
        # 
        lb_algorithm ="2CHOICES_UF",
        bucket_id= bucket_id, 
        tezcanalyticx_params= Some(
            TezcanalyticXParams(level=0)
        )
    )
    for i in range(100):
        x = client.put(bucket_id=bucket_id,key="hola-{}".format(i),value=b"HOLA").result()
        print(x)
    T.sleep(20)
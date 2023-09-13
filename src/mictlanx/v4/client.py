
import os
import json as J
import time as T
import requests as R
import hashlib as H
import logging as L
import magic as M
import numpy as np
import numpy.typing as npt
from option import Result,Ok,Err
from typing import List,Dict,Generator,Iterator,Awaitable,Set
from mictlanx.v4.interfaces.responses import PutMetadataResponse,PutResponse,GetMetadataResponse,GetBytesResponse,GetNDArrayResponse,Metadata
from mictlanx.logger.log import Log
from threading import Thread,Lock
from concurrent.futures import ThreadPoolExecutor,as_completed
from itertools import chain
from functools import reduce
from mictlanx.v4.interfaces.index import Peer,BallContext,PeerStats
from mictlanx.utils.segmentation import Chunks,Chunk
API_VERSION = 4 




class Client(object):
    # recommend worker is 4 
    def __init__(self,client_id:str,peers:List[Peer] = [], debug:bool=True,daemon:bool=True,max_workers:int = 4,algorithm:str="ROUND_ROBIN"):
        self.__algorithm     = algorithm
        self.__put_counter = 0
        self.__get_counter = 0
        self.__peers = peers
        self.client_id = client_id
        self.debug = debug
        self.__lock = Lock()
        self.__log         = Log(
            name = self.client_id,
            console_handler_filter = lambda record: self.debug
        )
        self.put_last_interarrival = 0
        self.put_arrival_time_sum:int = 0
        # 
        self.get_arrival_time_sum = 0
        self.get_last_interarrival = 0
        self.daemon = daemon

        # get current stats from peers
        # PeerID -> PeerStats
        self.__peer_stats:Dict[str, PeerStats] = {}
        self.__check_stats_peers(self.__peers)



        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers
        # PeerID -> u64
        self.put_operations_per_peer= {}
        # PeerID -> u64
        self.access_total_per_peer= {}
        
        # PeerID.Key -> u64
        self.replica_access_counter= {}
        
        # PeerId -> List[Key]
        self.keys_per_peer:Dict[str,Set[str]] = {}
        # Key -> BallContext
        self.balls_contexts:Dict[str,BallContext] = {}
        # BallID -> List[BallContext]
        self.chunk_map:Dict[str,List[BallContext]] = {}
        # PeerID -> f32
        # self.disk_ufs:Dict[str, float] = {}

        
        self.__thread_pool = ThreadPoolExecutor(max_workers= max_workers,thread_name_prefix="mictlanx-worker")
        self.__daemon_tick = 5

        self.enable_daemon = True

        if self.daemon:
            self.thread = Thread(target=self.__run,name="MictlanX")
            self.thread.setDaemon(True)
            self.thread.start()

    def __check_stats_peers(self, peers:List[Peer]):
        counter = 0
        for peer in peers:
            get_ufs_response = peer.get_ufs()
            if get_ufs_response.is_ok:
                response = get_ufs_response.unwrap()
                peer_stats = PeerStats(peer_id=peer.peer_id)
                peer_stats.total_disk = response.total_disk
                peer_stats.used_disk  = response.used_disk
                self.__log.debug("{}".format(peer_stats))
                self.__peer_stats[peer.peer_id] = peer_stats
                counter +=1
            else:
                self.__log.error("Peer {} is not available.".format(peer.peer_id))
        self.__log.debug("{}% of the peers are available".format( (counter / len(peers))*100,  ))
            

                # peer_stats.disk_uf    = 
            # print(peer.peer_id, get_ufs_response)

    def __global_operation_counter(self):
        return self.__get_counter + self.__put_counter

    def __lb(self,operation_type:str,algorithm:str="ROUND_ROBIN",key:str="",size:int= 0,peers:List[str]=[])->Peer:
        try:
            # print("PEERS",peers)
            filtered_peers = list(filter(lambda x: x.peer_id in peers,self.__peers))
            # print("FILTERED_PEERS",filtered_peers)
            if algorithm =="ROUND_ROBIN":
                return self.__lb_rb(operation_type,peers=filtered_peers)
            elif algorithm == "HASH":
                return self.__lb_hash(operation_type,key=key, peers=filtered_peers)
            elif algorithm == "PSEUDORANDOM":
                return self.__lb_pseudo_random(operation_type,key=key, peers=filtered_peers)
            elif algorithm == "2CHOICES":
                return self.__lb__two_choices(operation_type,key=key, peers=filtered_peers)
            else:
                return self.__lb_rb(operation_type,peers=filtered_peers)
        except Exception as e:
            self.__log.error("LB_ERROR "+str(e))

    def __lb__two_choices(self,operation_type:str,peers:List[Peer])-> Peer: 
        x = np.random.randint(0,len(peers))
        y = np.random.randint(0,len(peers))
        while x == y:
            x = np.random.randint(0,len(peers))
            y = np.random.randint(0,len(peers))

        peer_x = peers[x]
        peer_y = peers[y]

        if operation_type == "PUT":
            peer_x_put_counter = self.put_operations_per_peer.get(peer_x.peer_id,0)
            peer_y_put_counter = self.put_operations_per_peer.get(peer_y.peer_id,0)
            if peer_x_put_counter < peer_y_put_counter:
                return peer_x
            else:
                return peer_y
        elif operation_type == "GET":
            peer_x_get_counter = self.access_total_per_peer.get(peer_x.peer_id,0)
            peer_y_get_counter = self.access_total_per_peer.get(peer_y.peer_id,0)
            if peer_x_get_counter < peer_x_get_counter:
                return peer_x
            else:
                return peer_y
        else:
            return peer_x



        

        # peers
    def __lb_rb(self,operation_type:str,peers:List[Peer]=[])->Peer:
        x = self.__global_operation_counter()
        # print("RB_PEERs",peers)
        return peers[x % len(peers)]
    
    def __lb_hash(self,key:str, peers:List[Peer]=[])->Peer:
        return peers[hash(key) % len(peers)]

    def __lb_pseudo_random(self,key:str, peers:List[Peer]=[])->Peer:
        return peers[np.random.randint(0,len(peers))]


    def shutdown(self):
        self.thread.join()
        self.__thread_pool.shutdown(wait=True)

    def __run(self):
        while self.enable_daemon:
            if self.debug:
                global_counter = self.__put_counter + self.__get_counter 
                if global_counter >0:
                    put_avg_iat = self.put_arrival_time_sum / (self.__put_counter) if self.__put_counter >0 else 0
                    get_avg_iat = self.get_arrival_time_sum / (self.__get_counter) if self.__get_counter else 0
                    global_avg_iat = (self.get_arrival_time_sum + self.put_arrival_time_sum) / global_counter
                else:
                    put_avg_iat = 0
                    get_avg_iat = 0
                    global_avg_iat = 0
                
                # Get frecuencies
                # for peer_stats in self.__peers:
                #     if peer_stats.peer_id  in self.access_total_per_peer:
                #         Nx   = self.access_total_per_peer[peer_stats.peer_id]
                #         keys = self.keys_per_peer.get(peer_stats.peer_id,[])
                #         for key in keys:
                #             combined_key = "{}.{}".format(peer_stats.peer_id,key)
                #             if Nx > 0:
                #                 feq = self.replica_access_counter.get(combined_key,0)/Nx
                #                 print(peer_stats.peer_id,key,feq)

                            # if combined_key in self.access_counter_per_peer_key[combined_key]:
                                # feq = self.acc

                            # Reqx = self.access_counter_per_peer_key[]
                            # print("REQUEST",peer.node_id,Nx)
                # print("*"*20)
                    # for 
                    
                title = " ┬┴┬┴┤┬┴┬┴┤ MictlanX - Daemon ►_◄  ┬┴┬┴┤┬┴┬┴┤"
                print("|{:^50}|".format(title))
                print("-"*52)
                print(f"| {'PUT_COUNTER ':<43}| {'{}'.format(self.__put_counter):<4}|")
                print(f"| {'GET_COUNTER ':<43}| {'{}'.format(self.__get_counter):<4}|")
                print(f"| {'GLOBAL_COUNTER ':<43}| {'{}'.format(global_counter):<4}|")
                put_avg_at_formatted = "{:.2f}".format(put_avg_iat)
                get_avg_at_formatted = "{:.2f}".format(get_avg_iat)
                global_avg_at_formatted = "{:.2f}".format(global_avg_iat)
                print(f"| {'AVG_PUT_INTERARRIVAL ':<43}| {'{}'.format(put_avg_at_formatted):<4}|")
                print(f"| {'AVG_GET_INTERARRIVAL ':<43}| {'{}'.format(get_avg_at_formatted):<4}|")
                print(f"| {'AVG_GLOBAL_INTERARRIVAL ':<43}| {'{}'.format(global_avg_at_formatted):<4}|")
                print("-" * 52)
                for k,peer_stats in self.__peer_stats.items():
                    print(k,peer_stats)
                print("_"*50)
            T.sleep(self.__daemon_tick)

    def get_ndarray(self, key:str,to_disk:bool=False)->Awaitable[Result[GetNDArrayResponse,Exception]]:
        start_time = T.time()
        try:
            
            def __inner(get_result:Awaitable[Result[GetBytesResponse, Exception]])->Result[GetNDArrayResponse,Exception]:
                get_result = get_result.result()
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
                
            get_future_result =  self.get(key=key, to_disk=to_disk)
            return self.__thread_pool.submit(__inner, get_future_result)
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

    def get (self, key:str,to_disk:bool =False)->Awaitable[Result[GetBytesResponse,Exception]]:
        x = self.__thread_pool.submit(self.__get, key = key, to_disk = to_disk)
        return x

    def __get(self, key:str,to_disk:bool =False)->Result[GetBytesResponse,Exception]:
        try:
            start_time = T.time()
            if self.get_last_interarrival == 0:
                self.get_last_interarrival = start_time
            else:
                self.get_arrival_time_sum += (start_time - self.get_last_interarrival )
                self.get_last_interarrival = start_time
            

            
            if not key in self.balls_contexts:
                peer = self.__lb(operation_type="GET",algorithm=self.__algorithm,key=key,size=0,peers=list(map(lambda x: x.peer_id,self.__peers)))
                # self.__peers[hash(key) % len(self.__peers)]
            else:
                locations = self.balls_contexts[key].locations
                # print("LOCATIONS!",locations)
                peer = self.__lb(operation_type="GET", algorithm=self.__algorithm, key=key, size=0, peers=locations)
                # selected_peer_id = locations[self.__global_operation_counter() % len(locations)]
                # peer = next(filter(lambda x : x.peer_id == selected_peer_id, self.__peers), self.__lb())
            
            get_metadata_response = R.get("{}/api/v{}/metadata/{}".format(peer.http_url(),API_VERSION,key))

            get_metadata_response.raise_for_status()
            metadata_response = GetMetadataResponse(**get_metadata_response.json() )
            # _____________________________________________________________________________
            if metadata_response.node_id != peer.peer_id:
                peer = next(filter(lambda p: p.peer_id == metadata_response.node_id,self.__peers),Peer.empty())
                if(peer.port == -1):
                    return Err(Exception("{} not found".format(key)))
            
            # if not key in self.balls_contexts:
                # self
            response = R.get("{}/api/v{}/{}".format(peer.http_url(),API_VERSION,key))
            response.raise_for_status()
            response_time = T.time() - start_time
            self.__log.info("{} {} {} {}".format("GET", key,metadata_response.metadata.size,response_time))

            # Critical section
            with self.__lock:
                self.__peer_stats[peer.peer_id].get(key=key,size=metadata_response.metadata.size)       
                self.__get_counter+=1
                combined_key = "{}.{}".format(peer.peer_id,key)
                if not combined_key in self.replica_access_counter:
                    self.replica_access_counter[combined_key] = 1
                else:
                    self.replica_access_counter[combined_key] +=1

                if not peer.peer_id in self.access_total_per_peer:
                    self.access_total_per_peer[peer.peer_id] = 1 
                else:
                    self.access_total_per_peer[peer.peer_id] += 1
                
                #  CHECK IF KEY IS NOT IN BALL CONTEXT  AFTER A GET THEN ADDED 
                if not key in self.balls_contexts:
                    self.balls_contexts[key] = BallContext(size=metadata_response.metadata.size, locations=set([peer.peer_id] ))
                    self.keys_per_peer[peer.peer_id] = set([key])
                else:
                    if not metadata_response.node_id in self.balls_contexts[key].locations:
                        self.balls_contexts[key].locations.add(peer.peer_id)
                    

                    if not metadata_response.node_id in self.keys_per_peer.get(metadata_response.node_id,set([])):
                        self.keys_per_peer[metadata_response.node_id] = set([key])
                

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

    def put_chunks(self,key:str,chunks:Chunks,tags:Dict[str,str],checksum_as_key:bool= False,bucket_id:str="")->Generator[Result[PutResponse,Exception],None,None]:
        futures:List[Awaitable[Result[PutResponse,Exception]]] = []
        for i,chunk in enumerate(chunks.iter()):
            fut = self.put(
                value=chunk.data,
                tags={**tags, **chunk.metadata, "index": str(chunk.index), "checksum":chunk.checksum},
                key=chunk.chunk_id,
                checksum_as_key=checksum_as_key,
                ball_id=key,
                bucket_id=bucket_id
            )
            futures.append(fut)
        
        for result in as_completed(futures):
            res = result.result()
            if res.is_ok:
                chunk_ctx = BallContext(size= chunk.size, locations=[res.map(lambda x: x.node_id).unwrap()])
                if key in self.chunk_map:
                    self.chunk_map[key].append(chunk_ctx)
                else:
                    self.chunk_map[key] =[chunk_ctx]
            
            yield res
    
    def get_chunks_metadata(self,key:str,peer:Peer)->Result[Iterator[Metadata],Exception]:
        try:
            response = R.get("{}/api/v{}/metadata/{}/chunks".format(peer.http_url(),API_VERSION,key))
            response.raise_for_status()
            # print(response,peer)
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

    def get_and_merge_ndarray(self,key:str) -> Awaitable[Result[GetNDArrayResponse,Exception]]:
        return self.__thread_pool.submit(self.__get_and_merge_ndarray, key = key)
    
    def __get_and_merge_ndarray(self,key:str)->Result[GetNDArrayResponse,Exception] :
        start_time = T.time()
        res:Result[GetBytesResponse,Exception] = self.get_and_merge(key=key).result()
        if res.is_ok:
            response = res.unwrap()
            # print(
                # "BALL_ID={}".format(response.metadata),
                # "KEY={}".format(response.metadata.key),
                # str(response.metadata),
                # response.metadata.tags
            # )
            shapes_str = map( lambda x: eval(x), J.loads(response.metadata.tags.get("shape","[]")) )
            dtype_str  = next(map(lambda x: x,J.loads(response.metadata.tags.get("dtype","[]"))), "float32" )
            shapes_str = list(shapes_str)
            # print("SHAPE_STR",shapes_str)
            
            # map(lambda x: eval(x),J.loads(response.metadata.tags.get("shape","[]")))
            # print(dtype_str)
            # attributes = 0
            shape = list(shapes_str[0][:])
            shape[0] = 0

            for ss in shapes_str:
                shape[0]+= ss[0]
                
            # shape[1] = shapes_str[0][1]
            print("GENERATED_SHAPE",shape)
            ndarray       = np.frombuffer(response.value,dtype=dtype_str).reshape(shape)
            response_time = T.time() - start_time
            return Ok(GetNDArrayResponse(value=ndarray, metadata=response.metadata, response_time=response_time))

            # print("SHAPE",shape,"DTYPE",dtype_str)
            # print(shapes_str)
            # for key,value in response.metadata.tags:
                # if key == 
        else:
            return res
    
    def get_metadata_peers_async(self,key:str)->Generator[List[Metadata],None,None]:
        # metadatas_iters = []
        # metadatas:List[Metadata] = []
        if not key in self.chunk_map:
            for peer in self.__peers:
                metadata_result = self.get_chunks_metadata(key=key,peer=peer)
                if metadata_result.is_ok:
                    yield metadata_result.unwrap()
                    # metadatas_iters.append(metadata_result.unwrap())
            # metadatas = chain(*metadatas_iters)
        else:
            balls_ctxs:List[BallContext] = self.chunk_map.get(key,[])
            for ball_ctx in balls_ctxs:
                locations = ball_ctx.locations
                # flag      = False
                # while not flag:
                selected_location = self.__lb(operation_type="GET",algorithm=self.__algorithm, key=key, size=0, peers=locations)
                peer              = next(filter(lambda x: x.peer_id == selected_location.peer_id, self.__peers),  None)
                if not peer == None:
                    raise Exception("Peer not found in __peers")
                    # flag=True
                        

                metadata_result   = self.get_chunks_metadata(key=key, peer=peer)
                if metadata_result.is_ok:
                    # metadatas_iters.append(metadata_result.unwrap())
                    yield metadata_result.unwrap()
            # metadatas = chain(*metadatas_iters)

    def get_metadata_valid_index(self,metadata_fut_generator:Awaitable[Generator[List[Metadata],None,None]]): 
        metadatas = metadata_fut_generator.result()
        reduced_metadatas = reduce(lambda a,b: chain(a,b) ,metadatas)
        # chunks_metadata:List[Metadata] = []
        # print(reduced_metadatas)
        current_indexes = []
        for chunk_metadata in reduced_metadatas:
            index = int(chunk_metadata.tags.get("index","-1"))
            if index == -1:
                continue
            if not index in current_indexes:
                current_indexes.append(index)
                yield chunk_metadata
                # chunks_metadata.append(chunk_metadata)
        # if len(chunks_metadata) == 0:
            # return Err(Exception("{} not found".format(key)))

    def get_and_merge(self, key:str)->Awaitable[Result[GetBytesResponse,Exception]]:
        return self.__thread_pool.submit(self.__get_and_merge, key = key)
    
    def __get_and_merge(self,key:str)->Result[GetBytesResponse, Exception]:
        start_time = T.time()
        x               = self.__thread_pool.submit(Client.get_metadata_peers_async,self,key=key)
        chunks_metadata = self.__thread_pool.submit(Client.get_metadata_valid_index,self,metadata_fut_generator = x).result()
        results:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
        for chunk_metadata in chunks_metadata:
            # print("CHUNK_METADATA",chunk_metadata)
            res = self.get(key=chunk_metadata.key)
            results.append(res)
        # ______________________________________
        if len(results) == 0:
            return Err(Exception("Key={} not found".format(key)))

        #04. MERGE METADATA
        xs:List[GetBytesResponse] = []
        for chunk_metadata in as_completed(results):
            result:Result[GetBytesResponse,Exception] = chunk_metadata.result()
            # print("CHUNK_RESULT_METADATA",result)
            if result.is_ok:
                x:GetBytesResponse = result.unwrap()
                print(x.metadata.tags)
                print("_"*20)
                xs.append(x)
        xs = sorted(xs, key=lambda x: int(x.metadata.tags.get("index","-1")))
        merged_bytes = bytearray()
        checksum = ""
        response_time = T.time() - start_time
        size = 0
        tags = {}
        content_type = "application/octet-stream"
        producer_id = "MictlanX"
        print("XS_LEN",len(xs))
        for x in xs:
            print("SORTED_CHUNK_INDEX",x.metadata.tags["index"], x.metadata.tags["checksum"])
            size += x.metadata.size
            for key1,value in x.metadata.tags.items():
                if not key1  in tags:
                    tags[key1]=[value]
                else:
                    tags[key1].append(value)
            # 
            merged_bytes.extend(x.value)
        for key2,value in tags.items():
            tags[key2] = J.dumps(value)
            
        chunk_metadata = Metadata(key=key,size=size,checksum=checksum,tags=tags, content_type=content_type, producer_id=producer_id, ball_id=key)
        return Ok(GetBytesResponse(value=merged_bytes, metadata=chunk_metadata,response_time=response_time ))

    def put_ndarray(self, key:str, ndarray:npt.NDArray,tags:Dict[str,str],bucket_id:str="")->Awaitable[Result[PutResponse,Exception]]:
        try:
            value:bytes = ndarray.tobytes()
            dtype       = str(ndarray.dtype)
            shape_str   = str(ndarray.shape)
            return self.put(key=key, value=value,tags={**tags,"dtype":dtype,"shape":shape_str },bucket_id=bucket_id)
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

    def put(self,value:bytes,tags:Dict[str,str]={},checksum_as_key:bool=True,key:str="",ball_id:str ="",bucket_id:str="")-> Awaitable[Result[PutResponse,Exception]]:
        return self.__thread_pool.submit(self.__put,key=key, value=value, tags=tags,checksum_as_key=checksum_as_key,ball_id = key if ball_id == "" else ball_id,bucket_id=bucket_id)
    
    def __put(self,value:bytes,tags:Dict[str,str]={},checksum_as_key=False,key:str="",ball_id:str="",bucket_id:str="")->Result[PutResponse,Exception]:
        try:

            with self.__lock:
                self.__put_counter += 1
            start_time = T.time()
            
            if self.put_last_interarrival == 0:
                self.put_last_interarrival = start_time
            else:
                self.put_arrival_time_sum += (start_time - self.put_last_interarrival )
                self.put_last_interarrival = start_time
            

            if "checksum" in tags:
                if tags["checksum"] != "":
                    checksum = tags["checksum"]
                else:
                    h = H.sha256()
                    h.update(value)
                    checksum:str = h.hexdigest()
            else:
                h = H.sha256()
                h.update(value)
                checksum:str = h.hexdigest()
            
            key     = key if (not checksum_as_key or not key =="")  else checksum
            ball_id = key if ball_id == "" else ball_id
            size    = len(value)

            peer    = self.__lb(
                operation_type = "PUT",
                algorithm      = self.__algorithm,
                key            = key,
                peers          = list(map(lambda x: x.peer_id,self.__peers)),
                size           = size
            )
            

            content_type        = M.from_buffer(value,mime=True)
            if content_type == "application/octet-stream":
                # Double check
                c = M.from_buffer(value[:2048], mime=True)
                if not c =="application/octet-stream":
                    content_type = c

                
            
            put_metadata_result = peer.put_metadata(
                key= key, 
                size=size, 
                checksum=checksum,
                tags= tags,
                producer_id= self.client_id,
                content_type=content_type,
                ball_id=ball_id,
                bucket_id=bucket_id
            )
            if put_metadata_result.is_err:
                raise put_metadata_result.unwrap_err()
            put_metadata_response = put_metadata_result.unwrap()

            put_response = peer.put_data(task_id= put_metadata_response.task_id, key= key, value= value, content_type=content_type)
            if put_response.is_err:
                raise put_response.unwrap_err()
            
            response_time = T.time() - start_time
            self.__log.info("{} {} {} {}".format("PUT",key,size,response_time ))
            
            # _____________________________
            res = PutResponse(response_time=response_time,throughput= float(size) / float(response_time), node_id=peer.peer_id)

            with self.__lock:
            
                self.__peer_stats[peer.peer_id].put(key=key,size=size)
                if not peer.peer_id in self.put_operations_per_peer:
                    self.put_operations_per_peer[peer.peer_id] = 1 
                else:
                    self.put_operations_per_peer[peer.peer_id] += 1
                #  UPDATE balls_contexts
                # _________________________________________________________________________________
                if not key in self.balls_contexts:
                    self.balls_contexts[key] = BallContext(size=size, locations=set([peer.peer_id]))
                    self.keys_per_peer[peer.peer_id] = set([key])
                else:
                    self.keys_per_peer[peer.peer_id].add(key)
                    self.balls_contexts[key].locations.add(peer.peer_id)
                
                if not ball_id in self.chunk_map:
                    self.chunk_map[ball_id] = [ BallContext(size=size, locations=set([peer.peer_id]))]
                else:
                    self.chunk_map[ball_id].append(BallContext(size=size, locations=set([peer.peer_id])))

            
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
            with self.__lock:
                self.__put_counter-=1
            return Err(e)




if __name__ =="__main__":
    c=  Client(
        client_id="client-0",
        peers= [
            Peer(peer_id="mictlanx-sn-0", ip_addr="localhost", port=7000),
            Peer(peer_id="mictlanx-sn-1", ip_addr="localhost", port=7001),
        ],
        debug= True,
        daemon=True,
        max_workers=2
    )
    T.sleep(100)
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
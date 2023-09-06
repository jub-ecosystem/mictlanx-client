
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
from typing import List,Dict,Generator,Iterator,Awaitable
from mictlanx.v4.interfaces.responses import PutMetadataResponse,PutDataResponse,PutResponse,GetMetadataResponse,GetBytesResponse,GetNDArrayResponse,Metadata
from mictlanx.logger.log import Log
from threading import Thread,Lock
from concurrent.futures import ThreadPoolExecutor,as_completed
from itertools import chain
from functools import reduce
from mictlanx.v4.interfaces.index import Peer,BallContext
from mictlanx.utils.segmentation import Chunks,Chunk
API_VERSION = 4 
class Client(object):
    # recommend worker is 4 
    def __init__(self,client_id:str,peers:List[Peer] = [], debug:bool=True,daemon:bool=True,max_workers:int = 4,algorithm:str="ROUND_ROBIN"):
        self.algorithm = algorithm
        self.__put_counter = 0
        self.__get_counter = 0
        self.frequencies   = {}
        self.peers = peers
        self.client_id = client_id
        self.debug = debug
        self.lock = Lock()
        self.log         = Log(
            name = self.client_id,
            console_handler_filter = lambda record: self.debug
        )
        self.put_last_interarrival = 0
        self.put_arrival_time_sum:int = 0
        # 
        self.get_arrival_time_sum = 0
        self.get_last_interarrival = 0
        self.daemon = daemon

        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers
        # PeerID -> u64
        self.put_tasks_per_peer= {}
        # PeerID -> u64
        self.access_total_per_peer= {}
        # PeerID.Key -> u64
        self.access_counter_per_peer_key= {}
        # PeerId -> List[Key]
        self.keys_per_peer = {}
        self.balls_contexts:Dict[str,BallContext] = {}
        self.chunk_map:Dict[str,List[BallContext]] = {}
        
        self.thread_pool = ThreadPoolExecutor(max_workers= max_workers,thread_name_prefix="mictlanx-worker")
        self.daemon_tick = 5
        self.enable_daemon = True
        if self.daemon:
            self.thread = Thread(target=self.__run,name="MictlanX")
            self.thread.setDaemon(True)
            
            self.thread.start()

    def __global_operation_counter(self):
        return self.__get_counter + self.__put_counter

    def __lb(self,algorithm:str="ROUND_ROBIN",key:str="",size:int= 0)->Peer:
        if algorithm =="ROUND_ROBIN":
            return self.__lb_rb()
        elif algorithm == "HASH":
            return self.__lb_hash(key=key)
        else:
            return self.__lb_rb()

    def __lb_rb(self)->Peer:
        x = self.__global_operation_counter()
        return self.peers[x % len(self.peers)]
    
    def __lb_hash(self,key:str)->Peer:
        return self.peers[hash(key) % len(self.peers)]

    def shutdown(self):
        self.thread.join()
        self.thread_pool.shutdown(wait=True)

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
                for peer in self.peers:
                    if peer.node_id  in self.access_total_per_peer:
                        Nx   = self.access_total_per_peer[peer.node_id]
                        keys = self.keys_per_peer.get(peer.node_id,[])
                        for key in keys:
                            combined_key = "{}.{}".format(peer.node_id,key)
                            if Nx > 0:
                                feq = self.access_counter_per_peer_key.get(combined_key,0)/Nx
                                print(peer.node_id,key,feq)

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
            T.sleep(self.daemon_tick)

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
            return self.thread_pool.submit(__inner, get_future_result)
                # get_result.add_done_callback(Client.fx)
        except KeyError as e :
            self.log.error(str(e))
            return Err(e)
        except R.RequestException as e:
            self.log.error(str(e))
            return Err(e)
        except Exception as e:
            self.log.error(str(e))
            return Err(e)

    def get (self, key:str,to_disk:bool =False)->Awaitable[Result[GetBytesResponse,Exception]]:
        x = self.thread_pool.submit(self.__get, key = key, to_disk = to_disk)
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
                peer = self.peers[hash(key) % len(self.peers)]
            else:
                locations = self.balls_contexts[key].locations
                selected_peer_id = locations[self.__global_operation_counter() % len(locations)]
                peer = next(filter(lambda x : x.node_id == selected_peer_id, self.peers), self.__lb())
            
            get_metadata_response = R.get("{}/api/v{}/metadata/{}".format(peer.http_url(),API_VERSION,key))

            get_metadata_response.raise_for_status()
            metadata_response = GetMetadataResponse(**get_metadata_response.json() )
            # _____________________________________________________________________________
            if metadata_response.node_id != peer.node_id:
                peer = next(filter(lambda p: p.node_id == metadata_response.node_id,self.peers),Peer.empty())
                if(peer.port == -1):
                    return Err(Exception("{} not found".format(key)))
            
            # if not key in self.balls_contexts:
                # self
            response = R.get("{}/api/v{}/{}".format(peer.http_url(),API_VERSION,key))
            response.raise_for_status()
            response_time = T.time() - start_time
            self.log.info("{} {} {} {}".format("GET", key,metadata_response.metadata.size,response_time))

            # Critical section
            with self.lock:
                self.__get_counter+=1
                combined_key = "{}.{}".format(peer.node_id,key)
                if not combined_key in self.access_counter_per_peer_key:
                    self.access_counter_per_peer_key[combined_key] = 1
                else:
                    self.access_counter_per_peer_key[combined_key] +=1

                if not peer.node_id in self.access_total_per_peer:
                    self.access_total_per_peer[peer.node_id] = 1 
                else:
                    self.access_total_per_peer[peer.node_id] += 1
                
                #  CHECK IF KEY IS NOT IN BALL CONTEXT  AFTER A GET THEN ADDED 
                if not key in self.balls_contexts:
                    self.balls_contexts[key] = BallContext(size=metadata_response.metadata.size, locations=[peer.node_id ])
                    self.keys_per_peer[peer.node_id] = [key]
                else:
                    if not metadata_response.node_id in self.balls_contexts[key].locations:
                        self.balls_contexts[key].locations.append(peer.node_id)
                    if not metadata_response.node_id in self.keys_per_peer.get(metadata_response.node_id,[]):
                        self.keys_per_peer[metadata_response.node_id] = [key]
                

            return Ok(GetBytesResponse(value=response.content,metadata=metadata_response.metadata,response_time=response_time))

        except R.RequestException as e:
            self.log.error(str(e))
            headers = e.response.headers | {}
            error_msg = headers.get("error-message","UKNOWN_ERROR")
            self.log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.log.error(str(e))
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
            self.log.error(str(e))
            headers = e.response.headers
            error_msg = headers.get("error-message","UKNOWN_ERROR")
            self.log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.log.error(str(e))
            return Err(e)
        
        # pass

    def get_and_merge_ndarray(self,key:str) -> Awaitable[Result[GetNDArrayResponse,Exception]]:
        return self.thread_pool.submit(self.__get_and_merge_ndarray, key = key)
    
    def __get_and_merge_ndarray(self,key:str)->Result[GetNDArrayResponse,Exception] :
        start_time = T.time()
        res:Result[GetBytesResponse,Exception] = self.get_and_merge(key=key).result()
        if res.is_ok:
            response = res.unwrap()
            shape = [0,0]
            shapes_str = map(lambda x: eval(x),J.loads(response.metadata.tags.get("shape","[]")))
            dtype_str  = next(map(lambda x: x,J.loads(response.metadata.tags.get("dtype","[]"))), "float32" )
            # print(dtype_str)
            for (r,a) in shapes_str:
                shape[0]+= r
            shape[1] = a
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
            for peer in self.peers:
                metadata_result = self.get_chunks_metadata(key=key,peer=peer)
                if metadata_result.is_ok:
                    yield metadata_result.unwrap()
                    # metadatas_iters.append(metadata_result.unwrap())
            # metadatas = chain(*metadatas_iters)
        else:
            balls_ctxs:List[BallContext] = self.chunk_map.get(key,[])
            for ball_ctx in balls_ctxs:
                locations         = ball_ctx.locations
                selected_location = locations[self.__global_operation_counter() % len(locations)]
                peer              = next(filter(lambda x: x.node_id == selected_location, self.peers), self.__lb())
                metadata_result   = self.get_chunks_metadata(key=key, peer=peer)
                if metadata_result.is_ok:
                    # metadatas_iters.append(metadata_result.unwrap())
                    yield metadata_result.unwrap()
            # metadatas = chain(*metadatas_iters)

    def get_metadata_valid_index(self,metadata_fut_generator:Awaitable[Generator[List[Metadata],None,None]]): 
        metadatas = metadata_fut_generator.result()
        reduced_metadatas = reduce(lambda a,b: chain(a,b) ,metadatas)
        # chunks_metadata:List[Metadata] = []
        print(reduced_metadatas)
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
        return self.thread_pool.submit(self.__get_and_merge, key = key)
    
    def __get_and_merge(self,key:str)->Result[GetBytesResponse, Exception]:
        #01. GET METADATA FROM PEERS 
        # metadatas_iters = []
        # metadatas:List[Metadata] = []
        # if not key in self.chunk_map:
        #     for peer in self.peers:
        #         metadata_result = self.get_chunks_metadata(key=key,peer=peer)
        #         if metadata_result.is_ok:
        #             metadatas_iters.append(metadata_result.unwrap())
        #     metadatas = chain(*metadatas_iters)
        # else:
        #     balls_ctxs:List[BallContext] = self.chunk_map.get(key,[])
        #     for ball_ctx in balls_ctxs:
        #         locations         = ball_ctx.locations
        #         selected_location = locations[self.__global_operation_counter() % len(locations)]
        #         peer              = next(filter(lambda x: x.node_id == selected_location, self.peers), self.__lb())
        #         metadata_result   = self.get_chunks_metadata(key=key, peer=peer)
        #         if metadata_result.is_ok:
        #             metadatas_iters.append(metadata_result.unwrap())
        #     metadatas = chain(*metadatas_iters)
        x               = self.thread_pool.submit(Client.get_metadata_peers_async,self,key=key)
        chunks_metadata = self.thread_pool.submit(Client.get_metadata_valid_index,self,metadata_fut_generator = x).result()
        # print("GET METADATA FROM PEERS",y)
        # metadatas = []
        
        # __________________________________________________________________________________________________

        # 02. Check chunks if they has index tag. ( depends of the 01)
        # chunks_metadata:List[Metadata] = []
        # current_indexes = []
        # for chunk_metadata in metadatas:
        #     index = int(chunk_metadata.tags.get("index","-1"))
        #     if index == -1:
        #         continue
        #     if not index in current_indexes:
        #         current_indexes.append(index)
        #         chunks_metadata.append(chunk_metadata)
        # if len(chunks_metadata) == 0:
        #     return Err(Exception("{} not found".format(key)))
        # chunks_metadata = []
        # if len(chunks_metadata) == 0:
        #     return Err(Exception("{} not found".format(key)))
        # ________________________________________________________
        # 03. GET the chunks (depends of the 02)
        results:List[Awaitable[Result[GetBytesResponse,Exception]]] = []
        for chunk_metadata in chunks_metadata:
            res = self.get(key=chunk_metadata.key)
            results.append(res)
        # ______________________________________
        if len(results) == 0:
            return Err(Exception("Key={} not found".format(key)))

        #04. MERGE METADATA
        xs:List[GetBytesResponse] = []
        for chunk_metadata in as_completed(results):
            result:Result[GetBytesResponse,Exception] = chunk_metadata.result()
            if result.is_ok:
                xs.append(result.unwrap())
        xs = sorted(xs, key=lambda x: int(x.metadata.tags.get("index","-1")))
        merged_bytes = bytearray()
        checksum = ""
        response_time = 0
        size = 0
        tags = {}
        content_type = "application/octet-stream"
        producer_id = "MictlanX"
        for x in xs:
            size += x.metadata.size
            for key,value in x.metadata.tags.items():
                if not key  in tags:
                    tags[key]=[value]
                else:
                    tags[key].append(value)
            merged_bytes.extend(x.value)
        for key,value in tags.items():
            tags[key] = J.dumps(value)
            
        chunk_metadata = Metadata(key=key,size=size,checksum=checksum,tags=tags, content_type=content_type, producer_id=producer_id, ball_id=key)
        return Ok(GetBytesResponse(value=merged_bytes, metadata=chunk_metadata,response_time=response_time ))

    def put_ndarray(self, key:str, ndarray:npt.NDArray,tags:Dict[str,str],bucket_id:str="")->Awaitable[Result[PutResponse,Exception]]:
        try:
            value:bytes = ndarray.tobytes()
            dtype       = str(ndarray.dtype)
            shape_str   = str(ndarray.shape)
            return self.put(key=key, value=value,tags={**tags,"dtype":dtype,"shape":shape_str },bucket_id=bucket_id)
        except R.RequestException as e:
            self.log.error(str(e))
            headers = e.response.headers | {}
            error_msg = headers.get("error-message","UKNOWN_ERROR")
            self.log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.log.error(str(e))
            return Err(e)

    def put(self,value:bytes,tags:Dict[str,str]={},checksum_as_key:bool=True,key:str="",ball_id:str ="",bucket_id:str="")-> Awaitable[Result[PutResponse,Exception]]:
        return self.thread_pool.submit(self.__put,key=key, value=value, tags=tags,checksum_as_key=checksum_as_key,ball_id = key if ball_id == "" else ball_id,bucket_id=bucket_id)
    
    def __put(self,value:bytes,tags:Dict[str,str]={},checksum_as_key=False,key:str="",ball_id:str="",bucket_id:str="")->Result[PutResponse,Exception]:
        try:
            with self.lock:
                self.__put_counter += 1
            start_time = T.time()
            if self.put_last_interarrival == 0:
                self.put_last_interarrival = start_time
            else:
                self.put_arrival_time_sum += (start_time - self.put_last_interarrival )
                self.put_last_interarrival = start_time

            # storage_node = self.storage_nodes[(self.__put_counter + self.__get_counter) % len(self.storage_nodes) ]
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
            
            key = key if (not checksum_as_key or not key =="")  else checksum
            ball_id = key if ball_id == "" else ball_id

            peer = self.__lb(algorithm=self.algorithm,key = key)

                # self.get_tasks_per_peer.[storage_node.node_id]
            # print("Selected storage node",storage_node)
            content_type        = M.from_buffer(value,mime=True)
            size = len(value)
            put_metadata_response =R.post("{}/api/v{}/metadata".format(peer.http_url(),API_VERSION),json={
                "key":key,
                "size":size,
                "checksum":checksum,
                "tags":tags,
                "producer_id":self.client_id,
                "content_type":content_type,
                "ball_id":ball_id,
                "bucket_id":bucket_id
            })
            put_metadata_response.raise_for_status()
            put_metadata_response = PutMetadataResponse(**put_metadata_response.json())
            
            put_response = R.post(
                "{}/api/v{}/data/{}".format(peer.http_url(), API_VERSION,put_metadata_response.task_id),
                files= {
                    "upload":(key,value,content_type)
                },
                )
            put_response.raise_for_status()
            # put_data_response = PutDataResponse(**put_response.json())
            response_time = T.time() - start_time
            self.log.info("{} {} {} {}".format("PUT",key,size,response_time ))
            
            # _____________________________
            res = PutResponse(response_time=response_time,throughput= float(size) / float(response_time), node_id=peer.node_id)

            with self.lock:
            
                if not peer.node_id in self.put_tasks_per_peer:
                    self.put_tasks_per_peer[peer.node_id] = 1 
                else:
                    self.put_tasks_per_peer[peer.node_id] += 1
                #  UPDATE balls_contexts
                if not key in self.balls_contexts:
                    self.balls_contexts[key] = BallContext(size=size, locations=[peer.node_id ])
                    self.keys_per_peer[peer.node_id] = [key]
                else:
                    self.keys_per_peer[peer.node_id].append(key)
                    self.balls_contexts[key].locations.append(peer.node_id)
                
                if not ball_id in self.chunk_map:
                    self.chunk_map[ball_id] = [ BallContext(size=size, locations=[peer.node_id ])]
                else:
                    self.chunk_map[ball_id].append(BallContext(size=size, locations=[peer.node_id ]))

            
            return Ok(res)
            # print("PUT_RESPONSE",put_data_response.service_time+put_metadata_response.service_time,put_data_response.throughput)
            
        except R.RequestException as e:
            with self.lock:
                self.__put_counter-=1
            self.log.error(str(e))
            headers = e.response.headers | {}
            error_msg = headers.get("error-message","UKNOWN_ERROR")
            self.log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            with self.lock:
                self.__put_counter-=1
            self.log.error(str(e))
            return Err(e)




if __name__ =="__main__":
    c=  Client(
        client_id="client-0",
        peers= [
            Peer(node_id="mictlanx-sn-0", ip_addr="localhost", port=7000),
            Peer(node_id="mictlanx-sn-1", ip_addr="localhost", port=7001),
        ],
        debug= True,
        daemon=True,
        max_workers=2
    )
    # def test()
    futures:List[Awaitable[Result[GetNDArrayResponse, Exception]]] = []
    for i in range(10):
        # ndarray = np.random.randint(0,100,size=(10000,100))
        key     = "matrix-{}".format(i)
        # res     = c.put_ndarray(key=key,ndarray=ndarray,tags={"tag1":"tag1_value"})
        for i in range(np.random.randint(1,10)):
            fut = c.get_ndarray(key=key)
            # fut.add_done_callback(lambda x: print("FUTURE_RESULT",i, x.result()))
            futures.append(fut)
    T.sleep(60)
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
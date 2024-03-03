import os
from typing import List,Dict,Any,Set,Generator,AsyncGenerator,Iterator
from option import Result,Err,Ok,Option,NONE,Some
import json as J
from mictlanx.v4.interfaces.responses import PutMetadataResponse,GetUFSResponse,GetBucketMetadataResponse,PutChunkedResponse,GetMetadataResponse,Metadata,GetRouterBucketMetadataResponse
import time as T
import requests as R
from mictlanx.v4.xolo.utils import Utils as XoloUtils
import humanfriendly as HF
import httpx
from collections import namedtuple
# from mictlanx.utils.index import Utils



RouterBase = namedtuple("Router","router_id protocol ip_addr port")
class Router(RouterBase):
    
    def delete_by_ball_id(self,ball_id:str,bucket_id:str, timeout:int = 120,headers:Dict[str,str]={})->Result[str,Exception]:
        try:
            response = R.delete("{}/api/v{}/buckets/{}/bid/{}".format(self.base_url(),API_VERSION,bucket_id,ball_id),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(ball_id)
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)

    def get_chunks_metadata(self,key:str,bucket_id:str="",timeout:int= 60*2,headers:Dict[str,str]={})->Result[Iterator[Metadata],Exception]:
        # _key = Utils.sanitize_str(x=key)
        # _bucket_id = Utils.sanitize_str(x=bucket_id)
        # _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        try:
            response = R.get("{}/api/v{}/buckets/{}/metadata/{}/chunks".format(self.base_url(),API_VERSION,bucket_id,key),timeout=timeout,headers=headers)
            response.raise_for_status()
            chunks_metadata_json = map(lambda x: Metadata(**x) ,response.json())
            return Ok(chunks_metadata_json)
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)
        
    def delete(self,bucket_id:str,key:str,headers:Dict[str,str]={})->Result[bool,Exception]:
        try:
            response = R.delete(
                "{}/api/v{}/buckets/{}/{}".format(self.base_url(), 4 , bucket_id,key),
                headers=headers
            )
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    def disable(self,bucket_id:str, key:str,headers:Dict[str,str]={})->Result[bool, Exception]:
        try:
            response = R.post(
                "{}/api/v{}/buckets/{}/{}/disable".format(self.base_url(), 4 , bucket_id,key),
                headers=headers
            )
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    
    async def put_chuncked_async(self,task_id:str,chunks:AsyncGenerator[bytes, Any],timeout:int= 60*2,headers:Dict[str,str]={})->Result[PutChunkedResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id)
            async with httpx.AsyncClient() as client:
                put_response = await client.post(url=url,
                    data = chunks,
                    timeout = timeout,
                    # stream=True,
                    # headers=headers
                )
                put_response.raise_for_status()
                data = PutChunkedResponse(**J.loads(put_response.content))
                return  Ok(data)
            # put_response.raise_for_status()
            # data = PutChunkedResponse(**J.loads(put_response.content))
            # return  Ok(data)
        except Exception as e:
            return Err(e)


    def put_chuncked(self,task_id:str,chunks:Generator[bytes, None,None],timeout:int= 60*2,headers:Dict[str,str]={})->Result[PutChunkedResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id)
            put_response = R.post(url=url,
                data = chunks,
                timeout = timeout,
                stream=True,
                headers=headers
            )
            put_response.raise_for_status()
            json_response = J.loads(put_response.content)
            data = PutChunkedResponse(**json_response )
            return  Ok(data)
        except Exception as e:
            return Err(e)
    def empty():
        return Router(peer_id="",ip_addr="",port=-1)
    def get_addr(self)->str :
        return "{}:{}".format(self.ip_addr,self.port)
    def base_url(self):
        if self.port == -1 or self.port==0:
            return "{}://{}".format(self.protocol,self.ip_addr)
        return "{}://{}:{}".format(self.protocol,self.ip_addr,self.port)
    
    def get_metadata(self,bucket_id:str,key:str,timeout:int =300,headers:Dict[str,str]={})->Result[GetMetadataResponse,Exception]:
        try:
            # start_time = T.time()
            url = "{}/api/v{}/buckets/{}/metadata/{}".format(self.base_url(),4,bucket_id,key)
            # "{}/api/v{}/buckets/{}/metadata/{}".format(self.base_url(),API_VERSION,bucket_id,key)
            get_metadata_response = R.get(url, timeout=timeout,headers=headers)
            get_metadata_response.raise_for_status()
            response = GetMetadataResponse(**get_metadata_response.json() )
            # response_time = T.time() - start_time
            return Ok(response)
        except Exception as e:
            return Err(e)
    
    
    def get_streaming(self,bucket_id:str,key:str,timeout:int=300,headers:Dict[str,str]={})->Result[R.Response, Exception]:
        
        try:
            url = "{}/api/v{}/buckets/{}/{}".format(self.base_url(),4,bucket_id,key)
            get_response = R.get(url, timeout=timeout,stream=True,headers=headers)
            get_response.raise_for_status()
            return Ok(get_response)
        except Exception as e:
            return Err(e)
    def get_to_file(self,
                    bucket_id:str,
                    key:str,
                    chunk_size:str="1MB",
                    sink_folder_path:str="/mictlanx/data",
                    timeout:int=300,
                    filename:str="",
                    headers:Dict[str,str]={},
                    extension:str =""
    )->Result[str,Exception]:
        try:
            _chunk_size = HF.parse_size(chunk_size)
            # fullpath_base = "{}".format(sink_folder_path)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path,exist_ok=True)
            # fullpath = "{}/{}".format(fullpath_base,key)
            # _combined_key = "{}@{}".format(bucket_id,key)
            # combined_key = XoloUtils.sha256(_combined_key.encode("utf-8"))
            combined_key = XoloUtils.sha256("{}@{}".format(bucket_id,key).encode() ) if filename =="" else filename
            fullpath = "{}/{}{}".format(sink_folder_path,combined_key,extension)
            if os.path.exists(fullpath):
                return Ok(fullpath)
            # if os.path.exists(fullpath)
            url = "{}/api/v{}/buckets/{}/{}".format(self.base_url(),4,bucket_id,key)
            get_response = R.get(url, timeout=timeout,stream=True,headers=headers)
            get_response.raise_for_status()
            with open(fullpath,"wb") as f:
                for chunk in get_response.iter_content(chunk_size = _chunk_size):
                    if chunk:
                        f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)
        
    def put_metadata(self, 
                     key:str,
                     size:int,
                     checksum:str,
                     producer_id:str,
                     content_type:str,
                     ball_id:str,
                     bucket_id:str,
                     tags:Dict[str,str]={},
                     timeout:int= 60*2,
                     is_disable:bool = False,
                     headers:Dict[str,str]={}
    )->Result[PutMetadataResponse, Exception]:
            try:
                put_metadata_response =R.post("{}/api/v{}/buckets/{}/metadata".format(self.base_url(),4, bucket_id),json={
                    "key":key,
                    "size":size,
                    "checksum":checksum,
                    "tags":tags,
                    "producer_id":producer_id,
                    "content_type":content_type,
                    "ball_id":ball_id,
                    "bucket_id":bucket_id,
                    "is_disable":is_disable
                },
                timeout= timeout,headers=headers)
                put_metadata_response.raise_for_status()
                res_json = put_metadata_response.json()
                
                return Ok(PutMetadataResponse(
                    key= res_json.get("key","KEY"),
                    node_id=res_json.get("node_id","NODE_ID"),
                    service_time=res_json.get("service_time",-1),
                    task_id= res_json.get("task_id","0")
                 ))
            except Exception as e:
                return Err(e)
    def put_data(self,task_id:str,key:str, value:bytes, content_type:str,timeout:int= 60*2,headers:Dict[str,str]={},file_id:str="data") -> Result[Any, Exception]:
        try:
            put_response = R.post(
                "{}/api/v{}/buckets/data/{}".format(self.base_url(), 4,task_id),
                files= {
                    file_id:(key,value,content_type)
                },
                timeout = timeout,
                stream=True,
                headers=headers
            )
            put_response.raise_for_status()
            return  Ok(())
        except Exception as e:
            return Err(e)

    # def get_metadata(self)

    def get_bucket_metadata(self, bucket_id:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[GetRouterBucketMetadataResponse,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/metadata".format(self.base_url(), bucket_id)
                response = R.get(url=url, timeout=timeout,headers=headers)
                response.raise_for_status()
                x_json = response.json()
                return Ok(GetRouterBucketMetadataResponse(**x_json ))
        except Exception as  e:
            return Err(e)
    
    def delete(self,bucket_id:str,key:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[str,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/{}".format(self.base_url(), bucket_id,key)
                response = R.delete(url=url, timeout=timeout,headers=headers)
                
                response.raise_for_status()
                return Ok(key)
                # return Ok(GetBucketMetadataResponse(**response.json()))
        except Exception as  e:
            return Err(e)

    def get_ufs(self,timeout:int = 60*2,headers:Dict[str,str]={})->Result[GetUFSResponse, Exception]:
        try:
            response = R.get("{}/api/v4/stats/ufs".format(self.base_url()),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def __eq__(self, __value: "Router") -> bool:
        if not isinstance(__value, "Router"):
            return False
        return (self.ip_addr == __value.ip_addr and self.port == __value.port) or self.router_id == __value.router_id
    def __str__(self):
        return "Router(id = {}, ip_addr={}, port={})".format(self.router_id, self.ip_addr,self.port)
    
class Resources(object):
    def __init__(self,cpu:int=1, memory:str="1GB"):
        self.cpu = cpu
        self.memory = HF.parse_size(memory)


class Function(object):
    def __init__(self,key:str,image:str,resources:Resources,bucket_id:str="",keys:List[str]=[],endpoint_id:str=""):
        self.key       = key
        self.image     = image
        self.resources = resources
        self.bucket_id = bucket_id
        self.keys      = keys
        self.endpoint_id   = endpoint_id
class ProcessingStructure(object):
    def __init__(self,key:str, functions:List[Function],functions_order:Dict[str, List[str]],completed_functions:List[str] = []):
        self.key =  key
        self.functions = functions
        self.functions_order = functions_order
        self.completed_functions = completed_functions

class BallContext(object):
    def __init__(self,size:int, locations:Set[str]):
        self.size = size
        self.locations = locations
class DistributionSchema(object):
    def __init__(self):
        self.__schema:Dict[str, BallContext] = {}
        self.__chunks_schema:Dict[str, List[BallContext]] = {}

    def put(self,key:str,size:int,locations:Set[str] = set() ):
        self.__schema.setdefault(key,BallContext(size=size,locations=set()))
        self.__schema[key].locations.union(locations)
        
    def put_chunks(self,key:str, ball_contexts:List[BallContext]):
        self.__chunks_schema.setdefault(key, [])
        self.__chunks_schema[key] = ball_contexts
    


API_VERSION = 4 

class PeerStats(object):
    def __init__(self,peer_id:str): 
        self.__peer_id                 = peer_id
        self.total_disk:int            = 0
        self.used_disk                 = 0
        self.put_counter:int           = 0
        self.get_counter:int           = 0 
        self.balls                     = set()
        # 
        self.put_last_arrival_time     = -1
        self.put_sum_interarrival_time = 0
        
        self.get_last_arrival_time     = -1
        self.get_sum_interarrival_time = 0
        self.last_access_by_key:Dict[str,int]  = {}
        self.get_counter_per_key:Dict[str,int] = {}
        # self.put_frecuency:float = 0.0
        # self.get_frecuency:float = 0.0

    def put_frequency(self):
        x =  self.global_counter()
        if  x == 0:
            return 0
        return self.put_counter / x
    
    def get_frequency(self):
        x =  self.global_counter()
        if  x == 0:
            return 0
        return self.get_counter / x

    def get_frecuency_per_ball(self):
        res = {}
        for key, getcounter in self.get_counter_per_key.items():
            if self.get_counter == 0:
                res[key] = 0
            else:
                res[key] = getcounter / self.get_counter
        return res
    def top_N_by_freq(self,N:int):
        xs        = self.get_frecuency_per_ball()
        sorted_xs = list(sorted(xs.items(), key=lambda item: item[1], reverse=True))
        return sorted_xs[:N]
    def get_id(self):
        return self.__peer_id


    
    def put(self,key:str, size:int):
        self.put_counter+=1
        if not key in self.balls:
            self.get_counter_per_key[key] = 0
            self.used_disk+=size
        self.balls.add(key)

    def get(self, key:str, size:int):
        arrival_time = T.time()
        self.get_counter += 1
        self.last_access_by_key.setdefault(key,arrival_time)
        if not key in self.get_counter_per_key:
            self.get_counter_per_key[key] = 1
        else:
            self.get_counter_per_key[key] += 1 
        self.balls.add(key)
    def delete(self,key:str,size:int):
        self.balls.discard(key)
        if self.used_disk >=size:
            self.used_disk-=size
        del self.get_counter_per_key[key]
    
    def calculate_disk_uf(self,size:int = 0 ):
        return  1 - ((self.total_disk - (self.used_disk + size))/self.total_disk)
    
    def available_disk(self):
        return self.total_disk - self.used_disk

    def global_counter(self):
        return self.put_counter + self.get_counter
    
    def __str__(self):
        

        return "PeerStats(peer_id={}, total_disk={}, used_disk={}, available_disk={}, disk_uf={}, puts={}, gets={}, globals={}, put_feq={}, get_feq={}, topN={})".format(
            self.__peer_id,
            self.total_disk,
            self.used_disk,
            self.available_disk(),
            self.calculate_disk_uf(),
            self.put_counter,
            self.get_counter,
            self.global_counter(),
            self.put_frequency(),
            self.get_frequency(),
            self.top_N_by_freq(3)
            # self.get_frecuency_per_ball()
        )

    # ef 



class Peer(object):
    def __init__(self, peer_id:str, ip_addr:str, port:int,protocol:str="http"):
        self.peer_id = peer_id
        self.ip_addr = ip_addr
        self.port    = port
        self.protocol = protocol


    def delete_by_ball_id(self,ball_id:str,bucket_id:str, timeout:int = 120,headers:Dict[str,str]={})->Result[str,Exception]:
        try:
            response = R.delete("{}/api/v{}/buckets/{}/bid/{}".format(self.base_url(),API_VERSION,bucket_id,ball_id),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(ball_id)
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)
    def get_chunks_metadata(self,key:str,bucket_id:str="",timeout:int= 60*2,headers:Dict[str,str]={})->Result[Iterator[Metadata],Exception]:

        # _key = Utils.sanitize_str(x=key)
        # _bucket_id = Utils.sanitize_str(x=bucket_id)
        # _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        try:
            response = R.get("{}/api/v{}/buckets/{}/metadata/{}/chunks".format(self.base_url(),API_VERSION,bucket_id,key),timeout=timeout,headers=headers)
            response.raise_for_status()
            chunks_metadata_json = map(lambda x: Metadata(**x) ,response.json())
            return Ok(chunks_metadata_json)
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)
    def to_router(self):
        return Router(
            peer_id= self.peer_id,
            protocol=self.protocol,
            ip_addr= self.ip_addr,
            port=self.port
        )
    def disable(self,bucket_id:str, key:str,headers:Dict[str,str]={})->Result[bool, Exception]:
        try:
            response = R.post(
                "{}/api/v{}/buckets/{}/{}/disable".format(self.base_url(), 4 , bucket_id,key),
                headers=headers
            )
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    
    async def put_chuncked_async(self,task_id:str,chunks:AsyncGenerator[bytes, Any],timeout:int= 60*2,headers:Dict[str,str]={})->Result[PutChunkedResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id)
            async with httpx.AsyncClient() as client:
                put_response = await client.post(url=url,
                    data = chunks,
                    timeout = timeout,
                    # stream=True,
                    # headers=headers
                )
                put_response.raise_for_status()
                data = PutChunkedResponse(**J.loads(put_response.content))
                return  Ok(data)
            # put_response.raise_for_status()
            # data = PutChunkedResponse(**J.loads(put_response.content))
            # return  Ok(data)
        except Exception as e:
            return Err(e)


    def put_chuncked(self,task_id:str,chunks:Generator[bytes, None,None],timeout:int= 60*2,headers:Dict[str,str]={})->Result[PutChunkedResponse,Exception]:
        try:
            put_response = R.post(
                "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id),
                data = chunks,
                timeout = timeout,
                stream=True,
                headers=headers
            )
            put_response.raise_for_status()
            data = PutChunkedResponse(**J.loads(put_response.content))
            return  Ok(data)
        except Exception as e:
            return Err(e)
    def empty():
        return Router(peer_id="",ip_addr="",port=-1)
    def get_addr(self)->str :
        return "{}:{}".format(self.ip_addr,self.port)
    def base_url(self):
        if self.port == -1 or self.port==0:
            return "{}://{}".format(self.protocol,self.ip_addr)
        return "{}://{}:{}".format(self.protocol,self.ip_addr,self.port)
    
    def get_metadata(self,bucket_id:str,key:str,timeout:int =300,headers:Dict[str,str]={})->Result[GetMetadataResponse,Exception]:
        try:
            # start_time = T.time()
            url = "{}/api/v{}/buckets/{}/metadata/{}".format(self.base_url(),4,bucket_id,key)
            # "{}/api/v{}/buckets/{}/metadata/{}".format(self.base_url(),API_VERSION,bucket_id,key)
            get_metadata_response = R.get(url, timeout=timeout,headers=headers)
            get_metadata_response.raise_for_status()
            response = GetMetadataResponse(**get_metadata_response.json() )
            # response_time = T.time() - start_time
            return Ok(response)
        except Exception as e:
            return Err(e)
    
    
    def get_streaming(self,bucket_id:str,key:str,timeout:int=300,headers:Dict[str,str]={})->Result[R.Response, Exception]:
        
        try:
            url = "{}/api/v{}/buckets/{}/{}".format(self.base_url(),4,bucket_id,key)
            get_response = R.get(url, timeout=timeout,stream=True,headers=headers)
            get_response.raise_for_status()
            return Ok(get_response)
        except Exception as e:
            return Err(e)
    def get_to_file(self,bucket_id:str,key:str,chunk_size:str="1MB",sink_folder_path:str="/mictlanx/data",timeout:int=300,filename:str="",headers:Dict[str,str]={})->Result[str,Exception]:
        try:
            _chunk_size = HF.parse_size(chunk_size)
            # fullpath_base = "{}".format(sink_folder_path)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path,exist_ok=True)
            # fullpath = "{}/{}".format(fullpath_base,key)
            # _combined_key = "{}@{}".format(bucket_id,key)
            # combined_key = XoloUtils.sha256(_combined_key.encode("utf-8"))
            combined_key = XoloUtils.sha256("{}@{}".format(bucket_id,key).encode() ) if filename =="" else filename
            fullpath = "{}/{}".format(sink_folder_path,combined_key)
            # if os.path.exists(fullpath)
            url = "{}/api/v{}/buckets/{}/{}".format(self.base_url(),4,bucket_id,key)
            get_response = R.get(url, timeout=timeout,stream=True,headers=headers)
            get_response.raise_for_status()
            with open(fullpath,"wb") as f:
                for chunk in get_response.iter_content(chunk_size = _chunk_size):
                    if chunk:
                        f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)
        
    def put_metadata(self, 
                     key:str,
                     size:int,
                     checksum:str,
                     producer_id:str,
                     content_type:str,
                     ball_id:str,
                     bucket_id:str,
                     tags:Dict[str,str]={},
                     timeout:int= 60*2,
                     is_disable:bool = False,
                     headers:Dict[str,str]={}
    )->Result[PutMetadataResponse, Exception]:
            try:
                put_metadata_response =R.post("{}/api/v{}/buckets/{}/metadata".format(self.base_url(),4, bucket_id),json={
                    "key":key,
                    "size":size,
                    "checksum":checksum,
                    "tags":tags,
                    "producer_id":producer_id,
                    "content_type":content_type,
                    "ball_id":ball_id,
                    "bucket_id":bucket_id,
                    "is_disable":is_disable
                },
                timeout= timeout,headers=headers)
                put_metadata_response.raise_for_status()
                res_json = put_metadata_response.json()
                
                return Ok(PutMetadataResponse(
                    key= res_json.get("key","KEY"),
                    node_id=res_json.get("node_id","NODE_ID"),
                    service_time=res_json.get("service_time",-1),
                    task_id= res_json.get("task_id","0")
                 ))
            except Exception as e:
                return Err(e)
    def put_data(self,task_id:str,key:str, value:bytes, content_type:str,timeout:int= 60*2,headers:Dict[str,str]={},file_id:str="data") -> Result[Any, Exception]:
        try:
            put_response = R.post(
                "{}/api/v{}/buckets/data/{}".format(self.base_url(), 4,task_id),
                files= {
                    file_id:(key,value,content_type)
                },
                timeout = timeout,
                stream=True,
                headers=headers
            )
            put_response.raise_for_status()
            return  Ok(())
        except Exception as e:
            return Err(e)

    # def get_metadata(self)

    def get_bucket_metadata(self, bucket_id:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[GetBucketMetadataResponse,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/metadata".format(self.base_url(), bucket_id)
                response = R.get(url=url, timeout=timeout,headers=headers)
                response.raise_for_status()
                return Ok(GetBucketMetadataResponse(**response.json()))
        except Exception as  e:
            return Err(e)
    
    def delete(self,bucket_id:str,key:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[str,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/{}".format(self.base_url(), bucket_id,key)
                response = R.delete(url=url, timeout=timeout,headers=headers)
                
                response.raise_for_status()
                return Ok(key)
                # return Ok(GetBucketMetadataResponse(**response.json()))
        except Exception as  e:
            return Err(e)

    def get_ufs(self,timeout:int = 60*2,headers:Dict[str,str]={})->Result[GetUFSResponse, Exception]:
        try:
            response = R.get("{}/api/v4/stats/ufs".format(self.base_url()),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def __eq__(self, __value: "Peer") -> bool:
        if not isinstance(__value,"Peer") :
            return False
        return (self.ip_addr == __value.ip_addr and self.port == __value.port) or self.peer_id == __value.peer_id
    def __str__(self):
        return "Peer(id = {}, ip_addr={}, port={})".format(self.peer_id, self.ip_addr,self.port)



def check_destroyed(func):
    def wrapper(self,*args, **kwargs):
        if self._Ball__destroyed:
            raise Exception("{} was destroyed".format(self.key))
        result = func(self,*args, **kwargs)
        return result

    return wrapper

class Ball(object):
    def __init__(self,size:int, checksum:str,key:str="", path:Option[str]= NONE, value:bytes = bytes(),tags:Dict[str,str]={}, content_type:str="application/octet-stream") :
        self.size             = size
        self.content_type     = content_type
        self.key              = checksum if key =="" else key
        self.checksum         = checksum
        self.path:Option[str] = path
        self.__mictlanx_path  = "/mictlanx/client/.data/{}".format(self.checksum)
        self.value            = value
        self.tags             = tags
        self.__destroyed      = False
        # self.flushed:bool = if
    
    # @check_destroyed
    def __resolve_path(self,path:Option[str]=NONE)->str:
        return path.unwrap_or(self.path.unwrap_or(self.__mictlanx_path))
        # return self.path.unwrap_or(path.unwrap_or(self.__mictlanx_path))
    
    def from_bytes(key:str, value:bytes)->"Ball":
        size = len(value)
        content_type="application/octet-stream"
        # if size >= 2048:
            # content_type = M.from_buffer(value[:2048],mime=True)
        # else:
            # content_type = M.from_buffer(value[:],mime=True)
        
        checksum = XoloUtils.sha256(value=value)
        return Ball(key=key, size=size, checksum=checksum,value=value,content_type=content_type)
    
    def from_path(path:str,key:str="")->"Ball":
        if not os.path.exists(path):
            raise Exception("File at {} does not exists".format(path))
        (checksum, size) = XoloUtils.sha256_file(path)
        content_type="application/octet-stream"
        # if size >= 2048:
            # content_type = M.from_file(filename=path,mime=True)
        # else:
            # content_type = M.from_file(filename=path,mime=True)
        ball = Ball(key=key, checksum=checksum,size=size, path=Some(path),content_type=content_type)
        if os.path.exists(ball._Ball__mictlanx_path):
            ball.path = Some(ball._Ball__mictlanx_path)
        return ball
    
    @check_destroyed
    def to_disk(self,path:Option[str]= NONE, mictlanx_path:bool =True, clean:bool = True)->int:
        size = len(self.value)
        if size ==0:
            return -1
        _path = self.__resolve_path(path= Some (self.__mictlanx_path) if mictlanx_path else path )
        directory= os.path.dirname(_path)
        if not os.path.exists(path=directory):
            os.makedirs(directory)
        
        if os.path.exists(_path):
            return 1
        else:
            with open(_path,"wb") as f:
                f.write(self.value)
            if clean:
                self.clean()
            self.path = Some(self.__mictlanx_path)
            return 0

    @check_destroyed
    def to_memory(self,from_mictlanx:bool = True)->int:
        if from_mictlanx:
            self.read_all()
        # if mictlanx_path and os.path.exists(self.resolve_path()):
            
        if self.path.is_none:
            return -1
        else:
            self.value = self.read_all()
            return 0
    
    @check_destroyed
    def clean(self):
        self.value=b""

    @check_destroyed
    def destroy(self):
        self.clean()
        path = self.__resolve_path()
        if os.path.exists(path):
            print("Removed {}".format(path))
            # os.remove(path=path)
        self.__destroyed =True
        
    # def 

        # if path.is_none:
            
        # path:str = self.path.unwrap_or(path.unwrap_or("/mictlanx/client/.data/{}".format(self.checksum)))
        # self.va

    def read_all(self)->bytes:
        with open(self.__resolve_path(path = self.path),"rb") as f:
            return f.read()
        
    def read_gen(self,chunk_size:int=1024)->Generator[bytes, None, int]:
        with open(self.path,"rb") as f:
            size = 0
            while True:
                data = f.read(chunk_size)
                if not data:
                    return size
                size += len(data)
                yield data
    def __eq__(self, __value: "Ball") -> bool:
        return self.checksum == __value.checksum 

    def __str__(self):
        return "Ball(key={}, checksum={}, size={}, content_type={})".format(self.key,self.checksum,self.size,self.content_type)



if __name__ =="__main__":
    ps = ProcessingStructure(
        key="ps-0",
        functions=[
            Function(
                key="f1",
                image="nachocode/xolo:aes",
                resources=Resources(cpu=1,memory="1GB"),
                bucket_id="test-bucket-0",
                endpoint_id="disys0"
            ),
            Function(
                key="f2",
                image="nachocode/utils:lz4",
                resources=Resources(cpu=1,memory="1GB"),
                bucket_id="test-bucket-0",
                endpoint_id="disys1"
            ),
        ],
        functions_order={
            "f1":[],
            "f2":[]
        },
    )
    # pass
    # balls = 
    # small_ball = Ball.from_path(path="/source/01.pdf")
    # large_ball = Ball.from_path(path="/source/f155.mp4")
    # x= large_ball.to_memory()
    # print(x)
    # x= large_ball.to_disk()
    # print(x)
    # x = large_ball.to_memory()
    # print(x)
    # print(x)
    # large_ball.destroy()
    # T.sleep(3)
    # x = large_ball.to_memory()
    # print(x)
    
    # x = large_ball.to_disk()
    # print(x)
    # T.sleep(5)
    # large_ball.to_memory()
    

    # lbs:List[Ball] = []
    # for i in range(20):
    #     lbs.append(large_ball)
        
    # print(small_ball)
    # print(large_ball)
    # T.sleep(5)
    # print("Small")
    # small_ball.to_memory()
    # T.sleep(5)
    # print("LARGE")
    # for large_ball in lbs:
    #     large_ball.to_memory()
    # T.sleep(10)
    # print("CLEAN_MEMORY")
    # for large_ball in lbs:
    #     large_ball.clean()

    # b2   = Ball.from_bytes(key="",value=b1.read_all()+b"012012")
    # print(b2)
    # print(b1==b2)
    # print(ball.to_disk())
    # f = File("/source/01.pdf")
    # b = Ball(ball_id="BALL_ID")
    # b.add_file(key="f0",path="/source/01.pdf",force_update=False)
    # b.add_chunks_from_path(key="f0",path="/source/01.pdf",num_chunks=2)
    
    # print(b)
    # for data in f.read_gen():
        # print(data)
    # print(f.checksum,f.size)
    # print("A")
    # peer = Peer(peer_id="mictlanx-peer-0",ip_addr="localhost",port=7000)
    # m = peer.put_metadata( 
    #     key="test",
    #     size= 0,
    #     checksum="CHECKSUM",
    #     tags= {"EXAMPLE":"BALUE"},
    #     producer_id= "PRODUCER_I",
    #     content_type="application/octet-stream",
    #     ball_id="BALL_DI",
    #     bucket_id="BUCKE_ID"
    # )
    # print(m)

    # print(m)
    # print(
        # peer.put_data(task_id= m.task_id, key=m.key, value=b"HOLAAA" , content_type="application/octet-stream").unwrap_err().response.headers
    # )
    # print(peer.get_ufs().unwrap())

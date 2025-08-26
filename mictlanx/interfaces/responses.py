from typing import Dict ,Any,TypeVar,Generic,List,Optional
import numpy.typing as npt
from dataclasses import dataclass
from pydantic import BaseModel,Field
# from mictlanx.v4.interfaces.index import Ball
# from pydantic import BaseModel
# from typing import Generic,TypeVar,Dict,List
T = TypeVar("T")


# class ReplicationEvent(BaseModel):


@dataclass
class ElasticResponse:
    pool_size:int
    response_time:float
@dataclass
class ReplicationResponse:
    replication_event_id:str
    response_time:float
@dataclass
class DeleteBucketResponse:
    bucket_id: str
    deleted: int
    failed: int
    total:int
    keys:List[str]
    response_time: float
@dataclass
class BallContext:
    locations:List[str] 
    size:int

@dataclass
class PeerData:
    node_id:str
    ip_addr:str
    port:int
@dataclass
class PeerCurrentState:
    nodes:List[PeerData]
    balls:Dict[str, BallContext]

@dataclass
class BallBasicData:
    bucket_id:str
    key:str
    size:int

@dataclass
class StoragePeerResponse:
    id:str
    disk:int
    memory:int
    ip_addr:str
    port:int
    weight:float
    used_disk:int
    used_memory:int

@dataclass
class GetSizeByKey:
    bucket_id:str
    key:str
    peer_id:str
    size:int
    
@dataclass
class ReplicateResponse:
    peer_id:str
    replica_peer_id:str
    bucket_id:str
    key:str
    size:int
    ok:bool
    response_time:int

@dataclass
class DeletedResponse:
    n_deletes:int
    key_or_ball_id:str

@dataclass
class DeletedByBallIdResponse:
    n_deletes:int
    ball_id:str


@dataclass
class DeletedByKeyResponse:
    n_deletes:int
    key:str
@dataclass
class DeletedBallResponse:
    n_deletes:int
    ball_id:str


@dataclass
class BucketDeleteResponse:
    n_deleted_objects:int
    response_time:float

@dataclass
class PeerPutChunkedResponse:
    node_id:str
    combined_key:str
    bucket_id:str 
    key:str
    size:int
    throughput:float
    service_time:float
    
@dataclass
class PutChunkedResponse:
    bucket_id:str 
    key:str
    size:int
    replicas:List[str]
    throughput:float
    response_time:float
    
class Metadata(BaseModel):
    key:str # Unique identifier 
    size:int # Size in bytes of the data
    checksum:str # Sha256 checksum
    tags:Dict[str,str] # User-defined metadata
    content_type:str # Define the type of the content
    producer_id:str # Unique identifier of the user that allocate the data. 
    ball_id:str # Unique identifier used for segmentation purposes ball_id -> [chunk1, chunk2,...,chunkN]
    bucket_id:str = Field(default="") # Unique identifier used for MictlanX Sync
    is_disabled:bool=Field(default=False)

   
@dataclass
class AsyncGetResponse:
    data:memoryview
    metadatas:List[Metadata]

class BallMetadata(BaseModel):
    bucket_id:str
    ball_id:str
    size:str
    size_bytes:int
    checksum:str
    chunks:List[Metadata]
    

@dataclass
class PeerStatsResponse:
    peer_id:str
    used_disk:int
    total_disk:int 
    available_disk:int
    disk_uf:float
    balls:List[Metadata] 
    peers:List[str]
    def __eliminate_duplicates(self,metadata_list: List[Metadata]) -> List[Metadata]:
        seen = set()
        unique_metadata = []
        for metadata in metadata_list:
            identifier = (metadata.bucket_id, metadata.key)
            if identifier not in seen:
                seen.add(identifier)
                unique_metadata.append(metadata)
        return unique_metadata
    def to_dict(self):
        # Use asdict for simple fields, but manually convert the Metadata objects.
        data = self.__dict__.copy()
        data["balls"] = [ball.to_dict() for ball in self.balls]
        return data
    @staticmethod
    def empty()->'PeerStatsResponse':
        return PeerStatsResponse(
            peer_id="global",
            available_disk=0,
            balls=[],
            disk_uf=0,
            peers=[],
            total_disk=0,
            used_disk=0
        )
    def __add__(self,other:'PeerStatsResponse')->'PeerStatsResponse':
        return PeerStatsResponse(
            peer_id="global",
            used_disk= self.used_disk + other.used_disk,
            total_disk= self.total_disk + other.total_disk,
            available_disk=self.available_disk + other.available_disk,
            balls= self.__eliminate_duplicates(self.balls + other.balls),
            disk_uf= (self.disk_uf + other.disk_uf)/2,
            peers= list(set(self.peers + other.peers))
        )


class GetBucketMetadataResponse(object):
    def __init__(self,peer_id:str,balls:List[Dict[str,Any]]):
        self.balls = list(map(lambda ball: Metadata(**ball),balls))
        self.peer_id:str = peer_id
    def __str__(self):
        return "Bucket(n={})".format(len(self.balls))

class GetRouterBucketMetadataResponse(object):
    def __init__(self,bucket_id:str, peer_ids:List[str]=[], balls:List[Dict[str,Any]]={}, **kwargs):
        self.bucket_id = bucket_id
        self.balls = list(map(lambda ball: Metadata(**ball),balls))
        self.peer_ids:str = peer_ids
        self.extra = {**kwargs}
    def __str__(self):
        return "Bucket(n={})".format(len(self.balls))

class GetMetadataResponse(object):
    def __init__(self,service_time:int,peer_id:str,local_peer_id:str,metadata:Dict[str,Any]):
        self.service_time = service_time
        self.peer_id = peer_id
        self.local_peer_id = local_peer_id
        self.metadata = Metadata(**metadata)

class GetUFSResponse(object):
    def __init__(self,total_disk:int, used_disk:int, disk_uf:float):
        self.total_disk = total_disk
        self.used_disk  = used_disk
        self.disk_uf    = disk_uf
    def __str__(self):
        return "GetUFSResponse(total_disk={}, used_disk={}, disk_uf={})".format(self.total_disk, self.used_disk, self.disk_uf)

class PeerPutMetadataResponse(object):
    def __init__(self, 
                 key:str,
                 service_time:int,
                 task_id:str,
                 node_id:str 
    ):
        self.key = key 
        self.node_id = node_id 
        self.service_time = service_time 
        self.task_id = task_id

class PutMetadataResponse(object):
    def __init__(self, 
                 key:str,
                 service_time:int,
                 tasks_ids:List[str]=[],
                 bucket_id:str ="",
                 replicas:List[str]=[],
                 **kwargs
    ):
        self.bucket_id = bucket_id
        self.key = key 
        self.replicas = replicas
        self.service_time = service_time 
        self.tasks_ids = tasks_ids
class PutDataResponse(object):
    def __init__(self,service_time:int,throughput:float):
        self.service_time = service_time 
        self.throughput = throughput
# class GetResponse(object):
#     def __init__(self,)

class GetResponse(Generic[T]):
    def __init__(self,value:T, metadata:Metadata,response_time:int=-1):
        self.value:T           = value
        self.metadata:Metadata = metadata
        self.response_time     = response_time

class GetBytesResponse(GetResponse[bytes]):
    def __init__(self,value:bytes, metadata:Metadata,response_time:int=-1):
        super(GetBytesResponse,self).__init__(value=value,metadata=metadata,response_time=response_time)
    def __str__(self):
        return "GetResponse(response_time={}, size={})".format(self.response_time,len(self.value))
class GetNDArrayResponse(GetResponse[npt.NDArray]):
    def __init__(self,value:npt.NDArray, metadata:Metadata,response_time:int=-1):
        super(GetNDArrayResponse,self).__init__(value=value,metadata=metadata,response_time=response_time)
    def __str__(self):
        return "GetResponse(response_time={}, shape={})".format(self.response_time,self.value.shape)
    
class PutResponse(object):
    def __init__(self,response_time:int,throughput:float,replicas:List[str]=[],key:str=""):
        self.key           = key
        self.response_time = response_time
        self.replicas       = replicas
        self.throughput    = throughput

@dataclass
class GetToFileResponse:
    path:str
    metadata: Metadata
    response_time:float
    peer_id:str

@dataclass
class UpdateResponse:
    updated:bool
    bucket_id:str
    key:str
    replicas:List[str]
    throughput:float
    response_time:float 


class SummonResponse(BaseModel):
    container_id:str
    service_time:int
    ip_addr:str
    port:int

class SummonServiceResponse(BaseModel):
    id:str
    container_id: str
    created_at:int
    client_id:str
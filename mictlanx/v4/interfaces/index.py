import os
# import  aiofiles
from typing import List,Dict,Any,Set,Generator,AsyncGenerator,Iterator,Optional,Union
from option import Result,Err,Ok,Option,NONE,Some
import json as J
import mictlanx.v4.interfaces as InterfacesX
import time as T
import requests as R
from xolo.utils.utils import Utils as XoloUtils
import humanfriendly as HF
import httpx
from collections import namedtuple
from retry.api import retry_call
import ssl

VerifyType = Union[ssl.SSLContext,str,bool]

AvailableResourceBase = namedtuple("AvailableResource","ar_id protocol ip_addr port")
AvailableResourceIdBase = namedtuple("AvailableResourceId","cluster_id node_id")
class AvailableResourceId(AvailableResourceIdBase):
    def __str__(self):
        return "{}.{}".format(self.cluster_id,self.node_id)

class AvailableResource(AvailableResourceBase):
    @staticmethod
    def create(ar_id:AvailableResourceId,ip_addr:str, port:int= -1,protocol:str="http" )->"AvailableResource":
        return AvailableResource(protocol=protocol,ip_addr=ip_addr,port=port)
    def to_peer(self):
        return Peer(
            peer_id=str(self.ar_id),
            ip_addr=self.ip_addr,
            port=self.port,
            protocol=self.protocol,
        )


RouterBase = namedtuple("Router","router_id protocol ip_addr port")


class AsyncRouter:
    def __init__(self, router_id: str, ip_addr: str, port: int, protocol: str = "http",http2:bool=True):
        self.router_id = router_id
        self.ip_addr = ip_addr
        self.port = port
        self.protocol = protocol
        self.http2 = http2

    def get_addr(self) -> str:
        return f"{self.ip_addr}:{self.port}"

    def base_url(self) -> str:
        if self.port in (-1, 0):
            return f"{self.protocol}://{self.ip_addr}"
        return f"{self.protocol}://{self.ip_addr}:{self.port}"

    def __eq__(self, other: "Router") -> bool:
        if not isinstance(other, Router):
            return False
        return (self.ip_addr == other.ip_addr and self.port == other.port) or self.router_id == other.peer_id

    def __str__(self):
        return f"Router(id = {self.router_id}, ip_addr={self.ip_addr}, port={self.port})"

    @staticmethod
    def from_str(x: str) -> Result["Router", Exception]:
        parts = x.split(":")
        if len(parts) < 3:
            return Err(Exception(f"Invalid string: {x} - expected format <router_id>:<ip_addr>:<port>"))
        router_id, ip_addr, port = parts[0], parts[1], parts[2]
        protocol = "https" if int(port) <= 0 else "http"
        return Ok(Router(peer_id=router_id, ip_addr=ip_addr, port=int(port), protocol=protocol))
    @staticmethod
    def from_router(x:"Router")->'AsyncRouter':
        return AsyncRouter(router_id=x.router_id, ip_addr=x.ip_addr, port=x.port, protocol=x.protocol )

    async def add_peers(self, peers: List['Peer'], headers: Dict[str, str] = {}, timeout: int = 120) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/xpeers"
            xs = [{
                "protocol": p.protocol,
                "hostname": p.ip_addr,
                "port": p.port,
                "peer_id": p.peer_id
            } for p in peers]
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json=xs)
                response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    async def update_metadata(self, bucket_id: str, key: str, metadata: Any, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/u/buckets/{bucket_id}/{key}"
            data_json = metadata.__dict__
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.post(url, headers=headers, json=data_json)
                response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    

    async def get_streaming(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[httpx.Response, Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                # We return the response. The caller is responsible for streaming the data.
            return Ok(response)
        except Exception as e:
            return Err(e)
    async def get_to_file(self,
                          bucket_id: str,
                          key: str,
                          chunk_size: str = "1MB",
                          sink_folder_path: str = "/mictlanx/data",
                          timeout: int = 300,
                          filename: str = "",
                          headers: Dict[str, str] = {},
                          extension: str = "",
                          verify:VerifyType = False
                          ) -> Result[str, Exception]:
        try:
            # from humanfriendly import parse_size
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path, exist_ok=True)
            combined_key = filename if filename else XoloUtils.sha256(f"{bucket_id}@{key}".encode())
            fullpath = f"{sink_folder_path}/{combined_key}{extension}"

            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                async with client.stream("GET", f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/{key}", headers=headers) as response:
                    response.raise_for_status()
                    with open(fullpath, "wb") as f:
                        async for chunk in response.aiter_bytes(_chunk_size):
                            f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)    
    async def put_chunked(self, task_id: str, chunks: AsyncGenerator[bytes, Any], timeout: int = 120, headers: Dict[str, str] = {}, verify:VerifyType = False) -> Result[InterfacesX.PeerPutChunkedResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/data/{task_id}/chunked"
            async with httpx.AsyncClient(http2=self.http2,timeout=timeout,verify=verify) as client:
                put_response = await client.post(url, data=chunks, headers=headers)
                put_response.raise_for_status()
                data = InterfacesX.PeerPutChunkedResponse(**J.loads(put_response.content))
                return Ok(data)
        except Exception as e:
            return Err(e)   
    async def delete_by_ball_id(self, ball_id: str, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[InterfacesX.DeletedByBallIdResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/bid/{ball_id}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.delete(url, headers=headers)
                response.raise_for_status()
                content_data = response.json()
                return Ok(InterfacesX.DeletedByBallIdResponse(**content_data))
        except Exception as e:
            return Err(e)
    async def get_chunks_metadata(self, key: str, bucket_id: str = "", timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[InterfacesX.BallMetadata, Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/metadata/{key}/chunks"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                # Convert each JSON object into an instance of InterfacesX.Metadata
                data = InterfacesX.BallMetadata.model_validate(response.json())
                # chunks_metadata = map(lambda x: InterfacesX.Metadata(**x), data)
                return Ok(data)
        except Exception as e:
            return Err(e)
    async def delete(self, bucket_id: str, key: str, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[InterfacesX.DeletedByKeyResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(http2=True,verify=verify,timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                return Ok(InterfacesX.DeletedByKeyResponse(**json_data) )
        except Exception as e:
            return Err(e)
    async def disable(self, bucket_id: str, key: str, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/{key}/disable"
            async with httpx.AsyncClient(verify=verify,timeout=timeout) as client:
                response = await client.post(url, headers=headers)
                response.raise_for_status()
                return Ok(True)
        except Exception as e:
            return Err(e)
    async def put_metadata(self, key: str, size: int, checksum: str, producer_id: str, content_type: str,
                           ball_id: str, bucket_id: str, tags: Dict[str, str] = {}, timeout: int = 120,
                           is_disabled: bool = False, replication_factor: int = 1,
                           headers: Dict[str, str] = {},verify:VerifyType =False) -> Result[InterfacesX.PutMetadataResponse, Exception]:
            try:
                url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/metadata"
                data_json = {
                    "bucket_id": bucket_id,
                    "key": key,
                    "ball_id": ball_id,
                    "checksum": checksum,
                    "size": size,
                    "tags": tags,
                    "producer_id": producer_id,
                    "content_type": content_type,
                    "is_disabled": is_disabled,
                    "replication_factor": replication_factor
                }
                async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                    response = await client.post(url, json=data_json, headers=headers)
                    response.raise_for_status()
                    res_json = response.json()
                    return Ok(InterfacesX.PutMetadataResponse(
                        bucket_id=res_json.get("bucket_id", "BUCKET_ID"),
                        key=res_json.get("key", "KEY"),
                        replicas=res_json.get("replicas", []),
                        service_time=res_json.get("service_time", -1),
                        tasks_ids=res_json.get("tasks_ids", "0")
                    ))
            except Exception as e:
                return Err(e)
    async def put_data(self, task_id: str, key: str, value: bytes, content_type: str, timeout: int = 120,
                       headers: Dict[str, str] = {}, file_id: str = "data",verify:VerifyType = False) -> Result[Any, Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/data/{task_id}"
            # For file uploads, using httpx's 'files' parameter:
            files = {file_id: (key, value, content_type)}
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.post(url, files=files, headers=headers)
                response.raise_for_status()
            return Ok(())
        except Exception as e:
            return Err(e)
    async def get_bucket_metadata(self, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[InterfacesX.GetRouterBucketMetadataResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata"
            async with httpx.AsyncClient(timeout=timeout,verify =verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return Ok(InterfacesX.GetRouterBucketMetadataResponse(**response.json()))
        except Exception as e:
            return Err(e)
    async def get_ufs(self, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[InterfacesX.GetUFSResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/stats/ufs"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return Ok(InterfacesX.GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)
    async def get_metadata(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {},verify:VerifyType=False) -> Result[InterfacesX.GetMetadataResponse, Exception]:
        """
        Asynchronously fetches metadata for a given bucket and key.
        
        Constructs the URL from the base URL, API version, bucket_id, and key, and then sends a GET request.
        On success, it returns an Ok containing an instance of InterfacesX.GetMetadataResponse created from the JSON response.
        On failure, it returns an Err with the exception.
        """
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/metadata/{key}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
            metadata_obj = InterfacesX.GetMetadataResponse(**response.json())
            return Ok(metadata_obj)
        except Exception as e:
            return Err(e)
    async def get_by_checksum_to_file(
        self,
        checksum: str,
        chunk_size: str = "1MB",
        sink_folder_path: str = "/mictlanx/data",
        timeout: int = 300,
        filename: str = "",
        headers: Dict[str, str] = {},
        extension: str = "",
        verify:VerifyType = False
    ) -> Result[str, Exception]:
        """
        Downloads a file identified by a checksum and saves it to disk.
        
        Parameters:
        checksum: The checksum used to identify the file.
        chunk_size: The size of each chunk to read (e.g., "1MB").
        sink_folder_path: Folder path where the file will be saved.
        timeout: Timeout for the HTTP request.
        filename: Optional custom filename. If not provided, the checksum is used.
        headers: HTTP headers to include in the request.
        extension: File extension to append.
        
        Returns:
        A Result containing the full file path on success or an Exception on error.
        """
        try:
            # Convert the chunk size string into bytes
            _chunk_size = HF.parse_size(chunk_size)
            
            # Ensure the sink folder exists
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path, exist_ok=True)
            
            # Determine the filename to use (if none provided, use the checksum)
            combined_key = checksum if filename == "" else filename
            fullpath = f"{sink_folder_path}/{combined_key}{extension}"
            
            # If the file already exists, return its path
            if os.path.exists(fullpath):
                return Ok(fullpath)
            
            # Construct the URL for downloading the file by checksum
            url = f"{self.base_url()}/api/v{4}/buckets/checksum/{checksum}"
            
            # Create an asynchronous HTTP client
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                # Stream the GET response from the URL
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    # Open the destination file asynchronously for writing in binary mode
                    # async with aiofiles.open(fullpath, "wb") as f:
                    #     # Read and write chunks as they are received
                    #     async for chunk in response.aiter_bytes(_chunk_size):
                    #         if chunk:
                    #             await f.write(chunk)
            
            return Ok(fullpath)
        
        except Exception as e:
            return Err(e)

class Router(RouterBase):
    

    def add_peers(self, peers:List['Peer'],headers:Dict[str,str]={}, timeout:int=120):
        try:
            url = "{}/api/v4/xpeers".format(self.base_url())
            xs =list(map(lambda p: {
                "protocol":p.protocol,
                "hostname":p.ip_addr,
                "port":p.port,
                "peer_id":p.peer_id

            }, peers ))
            response = R.post(url=url, headers=headers,timeout=timeout,json=xs)
            print(response)
            response.raise_for_status()
        except Exception as e:
            pass

    def update_metadata(self,bucket_id:str, key:str, metadata:InterfacesX.Metadata, headers:Dict[str,str] ={}, timeout:int = 120)->Result[bool, Exception]:
        try:
            url = "{}/api/v4/u/buckets/{}/{}".format(self.base_url(),bucket_id,key)
            data_json = metadata.__dict__
            response = R.post(headers=headers,timeout=timeout,url=url,json=data_json)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)


    def replication(self,
        rf:int,
        bucket_id:str,
        key:str,
        from_peer_id:Optional[str] ="",
        protocol:Optional[str]="http",
        strategy:Optional[str]="ACTIVE",
        ttl:Optional[int]=1,
        headers:Dict[str,str]={},
        timeout:int =120
    )->Result[InterfacesX.ReplicateResponse,Exception]:
        try:
            url = "{}/api/v4/replication".format(self.base_url())
            data_json = {
                # "id":id,
                "rtype":"DATA",
                "rf":rf,
                "from_peer_id":from_peer_id,
                "bucket_id":bucket_id,
                "key":key,
                # "memory":memory,
                # "disk":disk,
                # "workers":workers,
                "protocol":protocol,
                "strategy":strategy,
                "ttl":ttl
            }
            response = R.post(headers=headers,timeout=timeout,url=url,json=data_json)
            response.raise_for_status()
            return Ok(InterfacesX.ReplicationResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def elastic(self,
            rf:int,
            # rtype:Optional[str] ="DATA",
            # from_peer_id:Optional[str]="",
            # bucket_id:Optional[str]="",
            # key:Optional[str]="",
            memory:Optional[int]=4000000000,
            disk:Optional[int]=40000000000,
            workers:Optional[int]=2,
            protocol:Optional[str]="http",
            strategy:Optional[str]="ACTIVE",
            ttl:Optional[int]=1,
            headers:Dict[str,str]={},
            timeout:int =120
    ):
        try:
            url = "{}/api/v4/elastic".format(self.base_url())
            response = R.post(headers=headers,timeout=timeout,url=url,json={
                "rtype":"SYSTEM",
                "rf":rf,
                # "from_peer_id":from_peer_id,
                # "bucket_id":bucket_id,
                # "key":key,
                "memory":memory,
                "disk":disk,
                "workers":workers,
                "protocol":protocol,
                "strategy":strategy,
                "ttl":ttl

            })
            response.raise_for_status()
            return Ok(InterfacesX.ElasticResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def elastic_async(self, replicationEvent:Any,headers:Dict[str,str]={}, timeout:int =120):
        try:
            url = "{}/elastic/async".format(self.base_url())
            response = R.post(headers=headers,timeout=timeout,url=url)
            response.raise_for_status()
        except Exception as e:
            return Err(e)
        
    def get_replica_map(self, headers:Dict[str,str]={}, timeout:int =120):
        try:
            url = "{}/replicamap".format(self.base_url())
            response = R.post(headers=headers,timeout=timeout,url=url)
            response.raise_for_status()
        except Exception as e:
            return Err(e)
    def delete_by_ball_id(self,ball_id:str,bucket_id:str, timeout:int = 120,headers:Dict[str,str]={})->Result[InterfacesX.DeletedByBallIdResponse,Exception]:
        try:
            response = R.delete("{}/api/v{}/buckets/{}/bid/{}".format(self.base_url(),API_VERSION,bucket_id,ball_id),timeout=timeout,headers=headers)
            response.raise_for_status()
            content_data = response.json()
            return Ok(InterfacesX.DeletedByBallIdResponse(**content_data))
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)

    def get_chunks_metadata(self,key:str,bucket_id:str="",timeout:int= 120,headers:Dict[str,str]={})->Result[Iterator[InterfacesX.Metadata],Exception]:

        try:
            response = R.get("{}/api/v{}/buckets/{}/metadata/{}/chunks".format(self.base_url(),API_VERSION,bucket_id,key),timeout=timeout,headers=headers)
            response.raise_for_status()
            chunks_metadata_json = map(lambda x: InterfacesX.Metadata(**x) ,response.json())
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
    
    async def put_chuncked_async(self,task_id:str,chunks:AsyncGenerator[bytes, Any],timeout:int= 60*2,headers:Dict[str,str]={})->Result[InterfacesX.PeerPutChunkedResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id)
            async with httpx.AsyncClient() as client:
                put_response = await client.post(url=url,
                    data = chunks,
                    timeout = timeout,
                )
                put_response.raise_for_status()
                data = InterfacesX.PeerPutChunkedResponse(**J.loads(put_response.content))
                return  Ok(data)
        except Exception as e:
            return Err(e)


    def put_chuncked(self,task_id:str,chunks:Generator[bytes, None,None],timeout:int= 120,headers:Dict[str,str]={})->Result[InterfacesX.PeerPutChunkedResponse,Exception]:
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
            data = InterfacesX.PeerPutChunkedResponse(**json_response )
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
    
    def get_metadata(self,bucket_id:str,key:str,timeout:int =300,headers:Dict[str,str]={})->Result[InterfacesX.GetMetadataResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/{}/metadata/{}".format(self.base_url(),4,bucket_id,key)
            get_metadata_response = R.get(url, timeout=timeout,headers=headers)
            get_metadata_response.raise_for_status()
            response = InterfacesX.GetMetadataResponse(**get_metadata_response.json() )
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
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path,exist_ok=True)
            combined_key = XoloUtils.sha256("{}@{}".format(bucket_id,key).encode() ) if filename =="" else filename

            fullpath = "{}/{}{}".format(sink_folder_path,combined_key,extension)

            if os.path.exists(fullpath):
                return Ok(fullpath)
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
        
    def get_by_checksum_to_file(self,
                    checksum:str,
                    chunk_size:str="1MB",
                    sink_folder_path:str="/mictlanx/data",
                    timeout:int=300,
                    filename:str="",
                    headers:Dict[str,str]={},
                    extension:str =""
    )->Result[str,Exception]:
        try:
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path,exist_ok=True)
       
            combined_key = checksum if filename =="" else filename
            fullpath = "{}/{}{}".format(sink_folder_path,combined_key,extension)
            if os.path.exists(fullpath):
                return Ok(fullpath)
            url = "{}/api/v{}/buckets/checksum/{}".format(self.base_url(),4,checksum)
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
                     is_disabled:bool = False,
                     replication_factor:int =1,
                     headers:Dict[str,str]={}
    )->Result[InterfacesX.PutMetadataResponse, Exception]:
            try:
                put_metadata_response =R.post("{}/api/v{}/buckets/{}/metadata".format(self.base_url(),4, bucket_id),json={
                    "bucket_id":bucket_id,
                    "key":key,
                    "ball_id":ball_id,
                    "checksum":checksum,
                    "size":size,
                    "tags":tags,
                    "producer_id":producer_id,
                    "content_type":content_type,
                    "is_disabled":is_disabled,
                    "replication_factor":replication_factor
                },
                timeout= timeout,headers=headers)
                put_metadata_response.raise_for_status()
                res_json = put_metadata_response.json()
                
                return Ok(InterfacesX.PutMetadataResponse(
                    bucket_id= res_json.get("bucket_id","BUCKET_ID"),
                    key= res_json.get("key","KEY"),
                    replicas=res_json.get("replicas",[]),
                    service_time=res_json.get("service_time",-1),
                    tasks_ids= res_json.get("tasks_ids","0"),
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

    def get_bucket_metadata(self, bucket_id:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[InterfacesX.GetRouterBucketMetadataResponse,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/metadata".format(self.base_url(), bucket_id)
                response = R.get(url=url, timeout=timeout,headers=headers)
                response.raise_for_status()
                x_json = response.json()
                return Ok(InterfacesX.GetRouterBucketMetadataResponse(**x_json ))
        except Exception as  e:
            return Err(e)
    
    def delete(self,bucket_id:str,key:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[InterfacesX.DeletedByKeyResponse,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/{}".format(self.base_url(), bucket_id,key)
                response = R.delete(url=url, timeout=timeout,headers=headers)
                response.raise_for_status()
                json_data = response.json()
                return Ok(
                    InterfacesX.DeletedByKeyResponse(**json_data)
                )
        except Exception as  e:
            return Err(e)

    def get_ufs(self,timeout:int = 60*2,headers:Dict[str,str]={})->Result[InterfacesX.GetUFSResponse, Exception]:
        try:
            response = R.get("{}/api/v4/stats/ufs".format(self.base_url()),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(InterfacesX.GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def __eq__(self, __value: "Router") -> bool:
        if not isinstance(__value, Router):
            return False
        return (self.ip_addr == __value.ip_addr and self.port == __value.port) or self.router_id == __value.router_id
    def __str__(self):
        return "Router(id = {}, ip_addr={}, port={})".format(self.router_id, self.ip_addr,self.port)

    @staticmethod
    def from_str(x:str)->Result['Router', Exception]:
        x_splitted = x.split(":")
        n = len(x_splitted)
        if n ==0 or n < 3:
            return Err(Exception("Invalid string: {} - the correct format is <router_id>:<ip_addr:<port>".format(x)))
        port = x_splitted[2]
        return Router(
            router_id=x_splitted[0],
            ip_addr=x_splitted[1],
            port=port,
            protocol= "https" if port <= 0 else "http"
        )
    
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
        )


class Peer(object):
    def __init__(self, peer_id:str, ip_addr:str, port:int,protocol:str="http"):
        self.peer_id = peer_id
        self.ip_addr = ip_addr
        self.port    = port
        self.protocol = protocol
    
    def flush_tasks(self,headers:Dict[str,str ]={}, timeout:int=120)->Result[bool,Exception]:
        try:
            url = "{}/api/v4/tasks".format(self.base_url())
            response = R.delete(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    def get_all_ball_sizes(self,headers:Dict[str,str ]={}, timeout:int=120,start:int=0, end:int =0):
        try:
            url = "{}/api/v4/xballs/size?start={}{}".format(self.base_url(), start,"" if end <=0 else "&end={}".format(end) )
            response = R.get(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            data_json = response.json()
            print("DATA_JHSON", data_json)
            return Ok([InterfacesX.BallBasicData(*x) for x in data_json])
        except Exception as e:
            return Err(e)

    def get_balls_len(self,headers:Dict[str,str ]={}, timeout:int=120)->Result[int, Exception]:
        try:
            url = "{}/api/v4/balls/len".format(self.base_url())
            response = R.get(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            data_json = response.json()
            return Ok(data_json.get("len",0))
        except Exception as e:
            return Err(e)
    

    def get_state(self,headers:Dict[str,str ]={}, timeout:int=120,start:int=0, end:int =0)->Result[InterfacesX.PeerCurrentState, Exception]:
        try:
            url = "{}/api/v4/peers/state?start={}{}".format(self.base_url(), start,"" if end <=0 else "&end={}".format(end) )
            response = R.get(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            data_json = response.json()
            body = InterfacesX.PeerCurrentState(
                nodes= list(map(lambda x:InterfacesX.PeerData(**x), data_json.get("nodes",[]))),
                balls=dict(list(map(lambda x : (x[0], InterfacesX.BallContext(**x[1]) ),   data_json.get("balls", {}).items() )))
            )
            return Ok(body)
        except Exception as e :
            return Err(e)
    def get_stats(self,headers:Dict[str,str ]={}, timeout:int=120,start:int=0, end:int =0)->Result[InterfacesX.PeerStatsResponse, Exception]:
        try:
            url = "{}/api/v4/stats?start={}{}".format(self.base_url(), start,"" if end <=0 else "&end={}".format(end) )
            response = R.get(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            data_json = response.json()
            body = InterfacesX.PeerStatsResponse(
                available_disk= data_json.get("available_disk",0), 
                balls= [ InterfacesX.Metadata(**b) for b in data_json.get("balls",[])], 
                disk_uf= data_json.get("disk_uf",0.0), 
                peer_id= data_json.get("peer_id","peer-id"),
                peers= data_json.get("peers",[]),
                total_disk= data_json.get("total_disk",0),
                used_disk= data_json.get("used_disk",0),
            )
            return Ok(body)
        except Exception as e :
            return Err(e)
    
    def get_balls(self,start:int=0, end:int=0,headers:Dict[str,str]={}, timeout=120)->Result[List[InterfacesX.BallBasicData], Exception]:
        try:
            url = "{}/api/v4/xballs?start={}{}".format(self.base_url(), start,"" if end <=0 else "&end={}".format(end)  )
            response = R.get(
                url= url, 
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()
            return Ok(list(map(lambda b: InterfacesX.BallBasicData(**b),response.json())))
        except Exception as e:
            return Err(e)

    def add_peer(self,id:str, disk:int, memory:int, ip_addr:str, port:int, weight:float, used_disk:int = 0, used_memory:int = 0, headers:Dict[str, str]={}, timeout:int = 3600)->Result[InterfacesX.StoragePeerResponse,Exception]:
        try:
            url      = "{}/api/v4/nodes".format(self.base_url())
            response = R.post(url, timeout=timeout, headers=headers, json={
                "id":id,
                "disk":disk,
                "memory":memory,
                "ip_addr":ip_addr,
                "port":port,
                "weight":weight,
                "used_disk":used_disk,
                "used_memory":used_memory,
            })
            response.raise_for_status()
            data_json = response.json()
            return Ok(InterfacesX.StoragePeerResponse(**data_json))
        except R.RequestException as e:
            if not e.response  == None:
                return Err(Exception(e.response.content.decode("utf-8")))
            else:
                return Err(Exception(str(e)))

        except Exception as e:
            return Err(e)
    def __add_peer(self,id:str, disk:int, memory:int, ip_addr:str, port:int, weight:float, used_disk:int = 0, used_memory:int = 0, headers:Dict[str, str]={}, timeout:int = 3600):
        result = self.add_peer(
            id= id,
            disk=disk,
            memory=memory,
            ip_addr=ip_addr,
            port=port,
            weight=weight,
            used_memory=used_memory,
            used_disk=used_disk,
            timeout=timeout,
            headers=headers
        )
        if result.is_err:
            raise result.unwrap_err()
        return result
    def add_peer_with_retry(self,
                            id:str,
                            disk:int,
                            memory:int,
                            ip_addr:str,
                            port:int,
                            weight:float,
                            used_disk:int = 0,
                            used_memory:int = 0,
                            headers:Dict[str, str]={},
                            timeout:int = 3600,
                            tries:int = 100,
                            delay:int = 1,
                            max_delay:int = 5,
                            jitter:float = 0.0,
                            backoff:float =1,
                            logger:Any = None
    )->Result[InterfacesX.StoragePeerResponse, Exception]:
        try:
            result = retry_call(
                self.__add_peer,
                fkwargs={
                    "id":id,
                    "disk":disk,
                    "memory":memory,
                    "ip_addr":ip_addr,
                    "port":port,
                    "weight":weight,
                    "used_disk":used_disk,
                    "used_memory":used_memory,
                    "timeout":timeout,
                    "headers": headers
                },
                tries=tries,
                delay=delay,
                max_delay=max_delay,
                jitter=jitter,
                backoff=backoff,
                logger=logger
            )
            return result
        except Exception as e:
            return Err(e)
    
        
    def replicate(self,bucket_id:str, key:str,timeout:int = 120,headers:Dict[str,str]={})->Result[InterfacesX.ReplicateResponse, Exception]:
        try:
            url      = "{}/api/v4/buckets/{}/{}/replicate".format(self.base_url(), bucket_id,key)
            response = R.post(url, timeout=timeout, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(InterfacesX.ReplicateResponse(**data_json))
        except R.RequestException as e:
            if not e.response == None:
                return Err(Exception(e.response.content.decode("utf-8")))
            return Err(Exception(e))
        except Exception as e:
            return Err(e)

    def get_size(self,bucket_id:str, key:str, timeout:int = 120,headers:Dict[str,str]={})->Result[InterfacesX.GetSizeByKey,Exception]:
        try:
            url      = "{}/api/v4/buckets/{}/{}/size".format(self.base_url(), bucket_id,key)
            response = R.get(url, timeout=timeout, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(InterfacesX.GetSizeByKey(
                **data_json
            ))
        except Exception as e:
            return Err(e)
    def delete(self,bucket_id:str,key:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[InterfacesX.DeletedByKeyResponse,Exception]:
        try:
            url      = "{}/api/v4/buckets/{}/{}".format(self.base_url(), bucket_id,key)
            response = R.delete(url=url, timeout=timeout,headers=headers)
            
            response.raise_for_status()
            return Ok(InterfacesX.DeletedByKeyResponse(
                n_deletes=int(response.headers.get("n-deletes",-1)),
                key=key
            ))
        except Exception as  e:
            return Err(e)
    def delete_by_ball_id(self,ball_id:str,bucket_id:str, timeout:int = 120,headers:Dict[str,str]={})->Result[InterfacesX.DeletedByBallIdResponse,Exception]:
        try:
            response = R.delete("{}/api/v{}/buckets/{}/bid/{}".format(self.base_url(),API_VERSION,bucket_id,ball_id),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(InterfacesX.DeletedByBallIdResponse(
                n_deletes=int(response.headers.get("n-deletes",-1)),
                ball_id=ball_id
            ))
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)
    def get_chunks_metadata(self,key:str,bucket_id:str="",timeout:int= 60*2,headers:Dict[str,str]={})->Result[Iterator[InterfacesX.Metadata],Exception]:

        try:
            response = R.get("{}/api/v{}/buckets/{}/metadata/{}/chunks".format(self.base_url(),API_VERSION,bucket_id,key),timeout=timeout,headers=headers)
            response.raise_for_status()
            chunks_metadata_json = map(lambda x:InterfacesX.Metadata(**x) ,response.json())
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
    
    async def put_chuncked_async(self,task_id:str,chunks:AsyncGenerator[bytes, Any],timeout:int= 60*2,headers:Dict[str,str]={})->Result[InterfacesX.PeerPutChunkedResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id)
            async with httpx.AsyncClient() as client:
                put_response = await client.post(url=url,
                    data = chunks,
                    timeout = timeout,
                )
                put_response.raise_for_status()
                data = InterfacesX.PeerPutChunkedResponse(**J.loads(put_response.content))
                return  Ok(data)
        except Exception as e:
            return Err(e)


    def put_chuncked(self,task_id:str,chunks:Generator[bytes, None,None],timeout:int= 60*2,headers:Dict[str,str]={})->Result[InterfacesX.PeerPutChunkedResponse,Exception]:
        try:
            put_response = R.post(
                "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id),
                data = chunks,
                timeout = timeout,
                stream=True,
                headers=headers
            )
            put_response.raise_for_status()
            data = InterfacesX.PeerPutChunkedResponse(**J.loads(put_response.content))
            return  Ok(data)
        except Exception as e:
            return Err(e)
    def empty():
        return Peer(peer_id="",ip_addr="",port=-1)
    def get_addr(self)->str :
        return "{}:{}".format(self.ip_addr,self.port)
    def base_url(self):
        if self.port == -1 or self.port==0:
            return "{}://{}".format(self.protocol,self.ip_addr)
        return "{}://{}:{}".format(self.protocol,self.ip_addr,self.port)
    
    def get_metadata(self,bucket_id:str,key:str,timeout:int =300,headers:Dict[str,str]={})->Result[InterfacesX.GetMetadataResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/{}/metadata/{}".format(self.base_url(),4,bucket_id,key)
            get_metadata_response = R.get(url, timeout=timeout,headers=headers)
            get_metadata_response.raise_for_status()
            response = InterfacesX.GetMetadataResponse(**get_metadata_response.json() )
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
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path,exist_ok=True)
            combined_key = XoloUtils.sha256("{}@{}".format(bucket_id,key).encode() ) if filename =="" else filename
            fullpath = "{}/{}".format(sink_folder_path,combined_key)
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
    )->Result[InterfacesX.PeerPutMetadataResponse, Exception]:
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
                
                return Ok(InterfacesX.PeerPutMetadataResponse(
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

    def get_bucket_metadata(self, bucket_id:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[InterfacesX.GetBucketMetadataResponse,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/metadata".format(self.base_url(), bucket_id)
                response = R.get(url=url, timeout=timeout,headers=headers)
                response.raise_for_status()
                return Ok(InterfacesX.GetBucketMetadataResponse(**response.json()))
        except Exception as  e:
            return Err(e)
    

    def get_ufs(self,timeout:int = 60*2,headers:Dict[str,str]={})->Result[InterfacesX.GetUFSResponse, Exception]:
        try:
            response = R.get("{}/api/v4/stats/ufs".format(self.base_url()),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(InterfacesX.GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def __get_ufs(self,timeout:int = 60*2,headers:Dict[str,str]={}):
        res = self.get_ufs(timeout=timeout,headers=headers)
        if res.is_err:
            raise res.unwrap_err()
        return res

    def get_ufs_with_retry(self,
                            timeout:int=60,
                            headers:Dict[str,str]={},
                            tries:int = 100,
                            delay:int = 1,
                            max_delay:int = 5,
                            jitter:float = 0.0,
                            backoff:float =1,
                            logger:Any = None
                           ):
        try:
            result = retry_call(
                self.__get_ufs,
                fkwargs={
                "timeout":timeout,
                "headers":headers
                },
                tries=tries,
                delay=delay,
                max_delay=max_delay,
                jitter=jitter,
                backoff=backoff,
                logger=logger,
            )
            return result
        except Exception as e:
            return Err(e)
        
    def __eq__(self, __value: "Peer") -> bool:
        if not isinstance(__value,Peer) :
            return False
        return (self.ip_addr == __value.ip_addr and self.port == __value.port) or self.peer_id == __value.peer_id
    def __str__(self):
        return "Peer(id = {}, ip_addr={}, port={})".format(self.peer_id, self.ip_addr,self.port)


class AsyncPeer(object):
    def __init__(self, peer_id: str, ip_addr: str, port: int, protocol: str = "http"):
        self.peer_id = peer_id
        self.ip_addr = ip_addr
        self.port = port
        self.protocol = protocol

    async def flush_tasks(self, headers: Dict[str, str] = {}, timeout: int = 120) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/tasks"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)

    async def get_all_ball_sizes(self, headers: Dict[str, str] = {}, timeout: int = 120, start: int = 0, end: int = 0) -> Result[List[InterfacesX.BallBasicData], Exception]:
        try:
            url = f"{self.base_url()}/api/v4/xballs/size?start={start}" + ("" if end <= 0 else f"&end={end}")
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            print("DATA_JSON", data_json)
            return Ok([InterfacesX.BallBasicData(*x) for x in data_json])
        except Exception as e:
            return Err(e)

    async def get_balls_len(self, headers: Dict[str, str] = {}, timeout: int = 120) -> Result[int, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/balls/len"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(data_json.get("len", 0))
        except Exception as e:
            return Err(e)

    async def get_state(self, headers: Dict[str, str] = {}, timeout: int = 120, start: int = 0, end: int = 0) -> Result[InterfacesX.PeerCurrentState, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/peers/state?start={start}" + ("" if end <= 0 else f"&end={end}")
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            body = InterfacesX.PeerCurrentState(
                nodes=[InterfacesX.PeerData(**x) for x in data_json.get("nodes", [])],
                balls={k: InterfacesX.BallContext(**v) for k, v in data_json.get("balls", {}).items()}
            )
            return Ok(body)
        except Exception as e:
            return Err(e)

    async def get_stats(self, headers: Dict[str, str] = {}, timeout: int = 120, start: int = 0, end: int = 0) -> Result[InterfacesX.PeerStatsResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/stats?start={start}" + ("" if end <= 0 else f"&end={end}")
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            body = InterfacesX.PeerStatsResponse(
                available_disk=data_json.get("available_disk", 0),
                balls=[InterfacesX.Metadata(**b) for b in data_json.get("balls", [])],
                disk_uf=data_json.get("disk_uf", 0.0),
                peer_id=data_json.get("peer_id", "peer-id"),
                peers=data_json.get("peers", []),
                total_disk=data_json.get("total_disk", 0),
                used_disk=data_json.get("used_disk", 0),
            )
            return Ok(body)
        except Exception as e:
            return Err(e)

    async def get_balls(self, start: int = 0, end: int = 0, headers: Dict[str, str] = {}, timeout: int = 120) -> Result[List[InterfacesX.BallBasicData], Exception]:
        try:
            url = f"{self.base_url()}/api/v4/xballs?start={start}" + ("" if end <= 0 else f"&end={end}")
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            return Ok([InterfacesX.BallBasicData(**b) for b in response.json()])
        except Exception as e:
            return Err(e)

    async def add_peer(self, id: str, disk: int, memory: int, ip_addr: str, port: int, weight: float, used_disk: int = 0, used_memory: int = 0, headers: Dict[str, str] = {}, timeout: int = 3600) -> Result[InterfacesX.StoragePeerResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/nodes"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json={
                    "id": id,
                    "disk": disk,
                    "memory": memory,
                    "ip_addr": ip_addr,
                    "port": port,
                    "weight": weight,
                    "used_disk": used_disk,
                    "used_memory": used_memory,
                })
            response.raise_for_status()
            data_json = response.json()
            return Ok(InterfacesX.StoragePeerResponse(**data_json))
        except httpx.HTTPError as e:
            if e.response is not None:
                return Err(Exception(e.response.text))
            else:
                return Err(Exception(str(e)))
        except Exception as e:
            return Err(e)

    async def __add_peer(self, id: str, disk: int, memory: int, ip_addr: str, port: int, weight: float, used_disk: int = 0, used_memory: int = 0, headers: Dict[str, str] = {}, timeout: int = 3600):
        result = await self.add_peer(
            id=id,
            disk=disk,
            memory=memory,
            ip_addr=ip_addr,
            port=port,
            weight=weight,
            used_disk=used_disk,
            used_memory=used_memory,
            timeout=timeout,
            headers=headers
        )
        if result.is_err:
            raise result.unwrap_err()
        return result

    async def add_peer_with_retry(
        self,
        id: str,
        disk: int,
        memory: int,
        ip_addr: str,
        port: int,
        weight: float,
        used_disk: int = 0,
        used_memory: int = 0,
        headers: Dict[str, str] = {},
        timeout: int = 3600,
        tries: int = 100,
        delay: int = 1,
        max_delay: int = 5,
        jitter: float = 0.0,
        backoff: float = 1,
        logger: Any = None
    ) -> Result[InterfacesX.StoragePeerResponse, Exception]:
        try:
            result =  await retry_call(
                self.__add_peer,
                fkwargs={
                    "id": id,
                    "disk": disk,
                    "memory": memory,
                    "ip_addr": ip_addr,
                    "port": port,
                    "weight": weight,
                    "used_disk": used_disk,
                    "used_memory": used_memory,
                    "timeout": timeout,
                    "headers": headers,
                },
                tries=tries,
                delay=delay,
                max_delay=max_delay,
                jitter=jitter,
                backoff=backoff,
                logger=logger,
            )
            return result
        except Exception as e:
            return Err(e)

    async def replicate(self, bucket_id: str, key: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[InterfacesX.ReplicateResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}/replicate"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(InterfacesX.ReplicateResponse(**data_json))
        except httpx.HTTPError as e:
            if e.response is not None:
                return Err(Exception(e.response.text))
            return Err(Exception(e))
        except Exception as e:
            return Err(e)

    async def get_size(self, bucket_id: str, key: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[InterfacesX.GetSizeByKey, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}/size"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(InterfacesX.GetSizeByKey(**data_json))
        except Exception as e:
            return Err(e)

    async def delete(self, bucket_id: str, key: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[InterfacesX.DeletedByKeyResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
            response.raise_for_status()
            return Ok(InterfacesX.DeletedByKeyResponse(
                n_deletes=int(response.headers.get("n-deletes", -1)),
                key=key
            ))
        except Exception as e:
            return Err(e)

    async def delete_by_ball_id(self, ball_id: str, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[InterfacesX.DeletedByBallIdResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/bid/{ball_id}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
            response.raise_for_status()
            return Ok(InterfacesX.DeletedByBallIdResponse(
                n_deletes=int(response.headers.get("n-deletes", -1)),
                ball_id=ball_id
            ))
        except httpx.HTTPError as e:
            return Err(e)
        except Exception as e:
            return Err(e)

    async def get_chunks_metadata(self, key: str, bucket_id: str = "", timeout: int = 120, headers: Dict[str, str] = {}) -> Result[Iterator[InterfacesX.Metadata], Exception]:
        try:
            url = f"{self.base_url()}/api/v{API_VERSION}/buckets/{bucket_id}/metadata/{key}/chunks"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            chunks_metadata_json = map(lambda x: InterfacesX.Metadata(**x), response.json())
            return Ok(chunks_metadata_json)
        except Exception as e:
            return Err(e)

    def to_router(self):
        # Converts this Peer to a Router (synchronous helper)
        return Router(
            peer_id=self.peer_id,
            protocol=self.protocol,
            ip_addr=self.ip_addr,
            port=self.port
        )

    async def disable(self, bucket_id: str, key: str, headers: Dict[str, str] = {}) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}/disable"
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)

    async def put_chunked(self, task_id: str, chunks: AsyncGenerator[bytes, Any], timeout: int = 120, headers: Dict[str, str] = {}) -> Result[InterfacesX.PeerPutChunkedResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/data/{task_id}/chunked"
            async with httpx.AsyncClient(timeout=timeout) as client:
                put_response = await client.post(url, data=chunks, headers=headers)
            put_response.raise_for_status()
            data = InterfacesX.PeerPutChunkedResponse(**J.loads(put_response.content))
            return Ok(data)
        except Exception as e:
            return Err(e)

    @staticmethod
    def empty():
        return Peer(peer_id="", ip_addr="", port=-1)

    def get_addr(self) -> str:
        return f"{self.ip_addr}:{self.port}"

    def base_url(self) -> str:
        if self.port == -1 or self.port == 0:
            return f"{self.protocol}://{self.ip_addr}"
        return f"{self.protocol}://{self.ip_addr}:{self.port}"

    async def get_metadata(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {}) -> Result[InterfacesX.GetMetadataResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata/{key}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                get_metadata_response = await client.get(url, headers=headers)
            get_metadata_response.raise_for_status()
            response = InterfacesX.GetMetadataResponse(**get_metadata_response.json())
            return Ok(response)
        except Exception as e:
            return Err(e)

    async def get_streaming(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[httpx.Response, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            return Ok(response)
        except Exception as e:
            return Err(e)

    async def get_to_file(self, bucket_id: str, key: str, chunk_size: str = "1MB", sink_folder_path: str = "/mictlanx/data", timeout: int = 300, filename: str = "", headers: Dict[str, str] = {}) -> Result[str, Exception]:
        try:
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path, exist_ok=True)
            combined_key = XoloUtils.sha256(f"{bucket_id}@{key}".encode()) if filename == "" else filename
            fullpath = f"{sink_folder_path}/{combined_key}"
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    with open(fullpath, "wb") as f:
                        async for chunk in response.aiter_bytes(_chunk_size):
                            if chunk:
                                f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)

    async def put_metadata(self, key: str, size: int, checksum: str, producer_id: str, content_type: str, ball_id: str, bucket_id: str, tags: Dict[str, str] = {}, timeout: int = 120, is_disable: bool = False, headers: Dict[str, str] = {}) -> Result[InterfacesX.PeerPutMetadataResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata"
            payload = {
                "key": key,
                "size": size,
                "checksum": checksum,
                "tags": tags,
                "producer_id": producer_id,
                "content_type": content_type,
                "ball_id": ball_id,
                "bucket_id": bucket_id,
                "is_disable": is_disable
            }
            async with httpx.AsyncClient(timeout=timeout) as client:
                put_metadata_response = await client.post(url, json=payload, headers=headers)
            put_metadata_response.raise_for_status()
            res_json = put_metadata_response.json()
            return Ok(InterfacesX.PeerPutMetadataResponse(
                key=res_json.get("key", "KEY"),
                node_id=res_json.get("node_id", "NODE_ID"),
                service_time=res_json.get("service_time", -1),
                task_id=res_json.get("task_id", "0")
            ))
        except Exception as e:
            return Err(e)

    async def put_data(self, task_id: str, key: str, value: bytes, content_type: str, timeout: int = 120, headers: Dict[str, str] = {}, file_id: str = "data") -> Result[Any, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/data/{task_id}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                put_response = await client.post(
                    url,
                    files={file_id: (key, value, content_type)},
                    headers=headers,
                )
            put_response.raise_for_status()
            return Ok(())
        except Exception as e:
            return Err(e)

    async def get_bucket_metadata(self, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[InterfacesX.GetBucketMetadataResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            return Ok(InterfacesX.GetBucketMetadataResponse(**response.json()))
        except Exception as e:
            return Err(e)

    async def get_ufs(self, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[InterfacesX.GetUFSResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/stats/ufs"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            return Ok(InterfacesX.GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)

    async def __get_ufs(self, timeout: int = 120, headers: Dict[str, str] = {}):
        res = await self.get_ufs(timeout=timeout, headers=headers)
        if res.is_err:
            raise res.unwrap_err()
        return res

    async def get_ufs_with_retry(self, timeout: int = 60, headers: Dict[str, str] = {}, tries: int = 100, delay: int = 1, max_delay: int = 5, jitter: float = 0.0, backoff: float = 1, logger: Any = None):
        try:
            result = await retry_call(
                self.__get_ufs,
                fkwargs={
                    "timeout": timeout,
                    "headers": headers
                },
                tries=tries,
                delay=delay,
                max_delay=max_delay,
                jitter=jitter,
                backoff=backoff,
                logger=logger,
            )
            return result
        except Exception as e:
            return Err(e)

    def __eq__(self, other: "Peer") -> bool:
        if not isinstance(other, Peer):
            return False
        return (self.ip_addr == other.ip_addr and self.port == other.port) or self.peer_id == other.peer_id

    def __str__(self):
        return f"Peer(id = {self.peer_id}, ip_addr = {self.ip_addr}, port = {self.port})"


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
    def __resolve_path(self,path:Option[str]=NONE)->str:
        return path.unwrap_or(self.path.unwrap_or(self.__mictlanx_path))
    
    def from_bytes(key:str, value:bytes)->"Ball":
        size = len(value)
        content_type="application/octet-stream"
        
        checksum = XoloUtils.sha256(value=value)
        return Ball(key=key, size=size, checksum=checksum,value=value,content_type=content_type)
    
    def from_path(path:str,key:str="")->"Ball":
        if not os.path.exists(path):
            raise Exception("File at {} does not exists".format(path))
        (checksum, size) = XoloUtils.sha256_file(path)
        content_type="application/octet-stream"
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
        self.__destroyed =True
        
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
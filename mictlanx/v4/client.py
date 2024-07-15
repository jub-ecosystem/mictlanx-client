
import os
import json as J
import time as T
import requests as R
from typing import  List,Dict,Generator,Awaitable,Tuple,Iterator,Any
import numpy as np
import numpy.typing as npt
import humanfriendly as HF
from option import Result,Ok,Err,Option,NONE,Some
import mictlanx.v4.interfaces as InterfaceX
from mictlanx.logger.log import Log
from threading import Lock
from concurrent.futures import ThreadPoolExecutor,as_completed
from itertools import chain,tee
from functools import reduce
from mictlanx.utils.segmentation import Chunks
from xolo.utils.utils import Utils as XoloUtils
from mictlanx.utils import Utils
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXHttpHandler,TezcanalyticXParams
from typing_extensions import deprecated
from pathlib import Path
from retry.api import retry_call


API_VERSION = 4 

class Client(object):
    """
    The MictlanX Client is an intuitive interface designed to facilitate seamless interactions between users and the MictlanX decentralized storage system. 
    This client provides essential functionalities that enable users to securely store (put) and retrieve (get) data with ease.
    """

    def __init__(
            self,
            client_id:str,
            bucket_id:str = "MICTLANX",
            debug:bool=True,
            max_workers:int = 12,
            lb_algorithm:str="ROUND_ROBIN",
            log_output_path:str = "/mictlanx/client",
            log_when:str="m",
            log_interval:int = 30,
            tezcanalyticx_params:Option[TezcanalyticXParams] = NONE,
            routers:List[InterfaceX.Router] = []
    ):
        """
        Initializes the Client with the following parameters: 

        Args:
            client_id (str): The unique identifier for the client.
            bucket_id (str): The bucket ID for storage. Defaults to "MICTLANX".
            debug (bool): Enables or disables debug mode. Defaults to True.
            max_workers (int): The maximum number of workers. Defaults to 12.
            lb_algorithm (str): The load balancing algorithm to use. Defaults to "ROUND_ROBIN".
            log_output_path (str): The path for log output. Defaults to "/mictlanx/client".
            log_when (str): The log rotation interval. Defaults to "m" (minutes).
            log_interval (int): The log rotation interval value. Defaults to 30.
            tezcanalyticx_params (Optional[TezcanalyticXParams]): Parameters for TezcanalyticX. Defaults to None.
            routers (List[InterfaceX.Router]): A list of router interfaces. Defaults to an empty list.  

        """
        self.client_id = client_id
        self.__lb_algorithm     = lb_algorithm
        self.__put_counter = 0
        # Total number of get operations
        self.__get_counter = 0
        # Peers
        self.__routers = routers
        self.__lock = Lock()
        self.__bucket_id = bucket_id
        
        # Log for basic operations
        self.__log         = Log(
            name = self.client_id,
            console_handler_filter =  lambda x: debug,
            # console_handler_filter,
            error_log=True,
            when=log_when,
            interval=log_interval,
            path= log_output_path,
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
        # PeerID -> PeerStats
        self.__peer_stats:Dict[str, InterfaceX.PeerStats] = {}
        
        if not os.path.exists(log_output_path):
            os.makedirs(name=log_output_path,mode=0o777,exist_ok=True)
        

        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers
        # PeerID -> u64
        self.__put_operations_per_peer= {}
        # PeerID -> u64
        self.__access_total_per_peer= {}
        
        self.__max_workers = max_workers
        self.__thread_pool = ThreadPoolExecutor(max_workers= self.__max_workers,thread_name_prefix="mictlanx-worker")


    def __log_response_error(self,e:R.HTTPError):
        if not e.response == None:
            msg = e.response.content.decode("utf-8"),
            status_code = e.response.status_code
        else:
            msg = str(e)
            status_code = 666
        self.__log.error({
            "msg":msg,
            "status_code":status_code
            })



    def get_streaming_with_retry(self,
                                key:str,
                                bucket_id:str="",
                                timeout:int=120,
                                chunk_size:str="1MB",
                                headers:Dict[str,str]={},
                                max_retries:int = 10,
                                delay:float =1,
                                max_delay:float = 2, 
                                backoff:float = 1,
                                jitter:float = 0,

    )->Result[Tuple[Iterator[bytes],InterfaceX.Metadata],Exception]:
        try:
            start_time  = T.time()
            _chunk_size = HF.parse_size(chunk_size)
            def __inner():
                _bucket_id   = self.__bucket_id if bucket_id =="" else bucket_id
                selected_peer = self.__lb(
                    operation_type = "GET",
                    algorithm      = self.__lb_algorithm,
                    key            = key,
                    size           = 0,
                    peers          = list(map(lambda x: x.router_id,self.__routers))
                )
                # _____________________________________________________________________________________________________________________
                get_metadata_result:Result[InterfaceX.GetMetadataResponse,Exception] = selected_peer.get_metadata(bucket_id=_bucket_id,key=key,timeout=timeout, headers=headers)
                # self.__get_metadata(bucket_id=bucket_id,key= key, peer=selected_peer,timeout=timeout)
                # .result()
                if get_metadata_result.is_err:
                    raise Exception(str(get_metadata_result.unwrap_err()))
                metadata_response = get_metadata_result.unwrap()
                metadata_service_time = T.time() - start_time
                # _________________________________________________________________________
                result = selected_peer.get_streaming(bucket_id=_bucket_id,key=key,timeout=timeout,headers=headers)
                if result.is_err:
                    raise result.unwrap_err()
                response_time = T.time() - start_time
                self.__log.info(
                    {
                        "event":"GET",
                        "bucket_id":bucket_id,
                        "key":key,
                        "size":metadata_response.metadata.size, 
                        "response_time":response_time,
                        "metadata_service_time":metadata_service_time,
                        "peer_id":metadata_response.peer_id,
                    }
                )
                return  (metadata_response.metadata,result.unwrap())
            (metadata,response) = retry_call(__inner,tries=max_retries,delay=delay,max_delay=max_delay,backoff=backoff,jitter=jitter)
            gen:Iterator[bytes] = response.iter_content(chunk_size=_chunk_size)
            return Ok((gen, metadata))
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
    def get_streaming(self,key:str,bucket_id:str="",timeout:int=120,chunk_size:str="1MB",headers:Dict[str,str]={})->Result[Tuple[Iterator[bytes],InterfaceX.Metadata],Exception]:
        """
        Retrieves data from the specified bucket using the given key as a streaming response.

        Args:
            key (str): The key for the data to be retrieved.
            bucket_id (str): The ID of the bucket from which data will be retrieved. Defaults to an empty string.
            timeout (int): The timeout for the operation in seconds. Defaults to 120 seconds.
            chunk_size (str): The size of each chunk to retrieve. Defaults to "1MB".
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.

        Returns:
            Result[Tuple[Iterator[bytes], InterfaceX.Metadata], Exception]: The result of the get operation, including either a streaming response with metadata or an exception.
        """
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
            get_metadata_result:Result[InterfaceX.GetMetadataResponse,Exception] = selected_peer.get_metadata(bucket_id=bucket_id,key=key,timeout=timeout, headers=headers)
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
            # value = bytearray()
            gen:Iterator[bytes] = response.iter_content(chunk_size=_chunk_size)
                # value.extend(chunk)
            # 
            self.__log.info(
                {
                    "event":"GET",
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":metadata_response.metadata.size, 
                    "response_time":response_time,
                    "metadata_service_time":metadata_service_time,
                    "peer_id":metadata_response.peer_id,
                }
            )
            return Ok((gen, metadata_response.metadata))
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
            # return Ok(InterfaceX.GetBytesResponse(value=bytes(value),metadata=metadata_response.metadata,response_time=response_time))



    def delete_bucket_async(self, bucket_id:str, headers:Dict[str,str]={}, timeout:int=120):
        start_time = T.time()
        deleted = 0 
        failed = 0  
        total =0 
        keys = []
        
        futures = []
        for metadata in self.get_all_bucket_metadata(bucket_id=bucket_id, headers=headers):
            total += len(metadata.balls)
            for ball in metadata.balls:
                start_time = T.time()
                del_fut = self.delete_async(key=ball.key,bucket_id=bucket_id,headers=headers,timeout=timeout)
                futures.append(del_fut)
        for fut in as_completed(futures):
            del_result:Result[InterfaceX.DeleteByKeyResponse, Exception] = fut.result()
            if del_result.is_ok:
     
                deleted +=1
            else:
                failed+=1
        rt = T.time() - start_time
        self.__log.info({
                "event":"DELETED.BUCKET",
                "bucket_id":bucket_id,
                "deleted":deleted,
                "failed": failed,
                "total": total,
                "response_time":rt
            })
        return Ok(
            InterfaceX.DeleteBucketResponse(
                bucket_id=bucket_id,
                deleted=deleted,
                failed= failed,
                total = total,
                keys=keys,
                response_time =rt
                #   T.time()- start_time
            )
        )

    
    def __get_ball_from_bucket(self,ball:InterfaceX.Metadata,output_folder_path:str):
        start_time = T.time()
        filename = ball.tags.get("fullname", ball.checksum)
        bucket_relative_path = ball.tags.get("bucket_relative_path",filename)
        path = Path(output_folder_path)/Path(bucket_relative_path)
        result = self.get_to_file(key=ball.key, bucket_id=ball.bucket_id, filename=filename, output_path=output_folder_path)
        return start_time,ball,result

    

    def get_bucket_data_iter(self,
                            bucket_id:str,
                            chunk_size:str="1MB",
                            headers:Dict[str,str]={},
                            max_retries:int = 10,
                            delay:float =1,
                            max_delay:float = 2, 
                            backoff:float = 1,
                            jitter:float = 0,

    )->Generator[Tuple[Iterator[bytes], InterfaceX.Metadata], Any, Any]:
        try:
            # os.makedirs(name=output_folder_path,exist_ok=True)
            gen_buckets_replicas= self.get_all_bucket_metadata(bucket_id=bucket_id, headers=headers)
            # res:List[str] = []
            # futures =[]
            # with ThreadPoolExecutor(max_workers=_max_workers) as tp:
            already_get = []
            for bucket_replica in gen_buckets_replicas:
                for ball in bucket_replica.balls:
                    combined_key = "{}@{}".format(ball.bucket_id, ball.key)
                    if ball.key in already_get:
                        continue
                    already_get.append(combined_key)
                    get_res = self.get_streaming_with_retry(key=ball.key, bucket_id=ball.bucket_id,chunk_size=chunk_size,max_retries=max_retries,delay=delay,max_delay=max_delay,backoff=backoff,jitter=jitter)
                    if get_res.is_ok:
                        (gen, metadata) = get_res.unwrap()
                        yield (gen,metadata)
        except Exception as e:
            self.__log_response_error(e)
            return Err(e)

    def get_bucket_data(self,bucket_id:str,output_folder_path:str="/sink",headers:Dict[str,str]={})->Result[List[str],Exception]:
        try:
            os.makedirs(name=output_folder_path,exist_ok=True)
            gen_buckets_replicas= self.get_all_bucket_metadata(bucket_id=bucket_id, headers=headers)
            res:List[str] = []
            futures =[]
            # with ThreadPoolExecutor(max_workers=_max_workers) as tp:
            already_get = []
            for bucket_replica in gen_buckets_replicas:
                for ball in bucket_replica.balls:
                    if ball.key in already_get:
                        continue
                    already_get.append(ball.key)
                    fut = self.__thread_pool.submit(self.__get_ball_from_bucket, ball, output_folder_path)
                    futures.append(fut)
            for fut in as_completed(futures):
                xstart_time,ball,result = fut.result()
                if result.is_ok:
                    y = result.unwrap()
                    self.__log.info({
                        "event":"GET.BALL",
                        "bucket_id":bucket_id,
                        "key":ball.key,
                        "path":y,
                        "response_time":T.time()-xstart_time
                    })
                    res.append(str(y))
            return Ok(res)
        except Exception as e:
            self.__log_response_error(e)
            return Err(e)
    def disable(self,bucket_id:str, key:str,headers:Dict[str,str]={})->List[Result[bool,Exception]]:
        try:
            key = Utils.sanitize_str(key)
            ress = []
            for peer in self.__routers:
                res = peer.disable(bucket_id=bucket_id,key=key,headers=headers)
                ress.append(res)
            return ress
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
        

    # ================================ PUTS ============================
    def put_encrypt(self,
            bucket_id:str,
            value:bytes,
            secret_key:bytes,
            secret_header:Option[bytes] = NONE,
            key:str="",
            ball_id:str ="",
            tags:Dict[str,str]={},
            chunk_size:str="1MB",
            timeout:int = 120,
            disabled:bool=False,
            content_type:str = "application/octet-stream",
            headers:Dict[str,str]={}
    ):
        try:
            encrypted_data = XoloUtils.encrypt_aes(key=secret_key,data=value, header=secret_header)
            if encrypted_data.is_err:
                return Err(encrypted_data.unwrap_err())
            return self.put(
                bucket_id=bucket_id,value=encrypted_data.unwrap(), key=key, ball_id=ball_id,tags=tags,chunk_size=chunk_size,
                timeout=timeout,disabled=disabled, content_type=content_type, headers=headers,
            )
            
        except Exception as e:
            return Err(e)
    def put_chunked(self, chunks:Generator[bytes,None,None], 
                         bucket_id:str="",
                         key:str="",
                         ball_id:str="",
                        #  peer_id:Option[str]=NONE,
                         tags:Dict[str,str]={},
                         timeout:int=30,
                         disabled:bool=False,
                         content_type:str = "application/octet-stream",
                         replication_factor:int =1,
                         headers:Dict[str,str] = {},
                         
                   )-> Result[InterfaceX.PutChunkedResponse, Exception] : 
        try:
            start_time = T.time()
            _chunks,chunks2 =  tee(chunks, 2 )
            (checksum,size) = XoloUtils.sha256_stream(chunks2)
            _key     = (key if (not key =="")  else checksum)
            with self.__lock:
                # if peer_id.is_some:
                #     _peers = [peer_id.unwrap()]
                # else:
                _peers = list(map(lambda x: x.router_id,self.__routers))
                # ______________________________________________________
                router    = self.__lb(
                    operation_type = "PUT",
                    algorithm      = self.__lb_algorithm,
                    key            = _key,
                    peers          = _peers,
                    size           = size
                )
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            ball_id = _key if ball_id =="" else ball_id
            put_metadata_start_time = T.time()
            put_metadata_result = router.put_metadata(
                key          = _key, 
                size         = size, 
                checksum     = checksum,
                tags         = tags,
                producer_id  = self.client_id,
                content_type = content_type,
                ball_id      = ball_id,
                bucket_id    = _bucket_id,
                timeout      = timeout,
                is_disabled   = disabled,
                headers= headers,
                replication_factor=replication_factor
            )
            if put_metadata_result.is_err:
                raise put_metadata_result.unwrap_err()
            


            put_metadata_response = put_metadata_result.unwrap()
            put_metadata_rt = T.time() - put_metadata_start_time
            self.__log.info({
                "event":"PUT.METADATA",
                "bucket_id":_bucket_id,
                "key":_key,
                "replicas":put_metadata_response.replicas,
                "service_time":put_metadata_response.service_time,
                "reponse_time":put_metadata_rt
            })
            failed = []
            success = []
            datas = tee(_chunks,len(put_metadata_response.replicas))
            for task_id, peer_id,chunks in zip(put_metadata_response.tasks_ids, put_metadata_response.replicas,datas):
                # self.__put_response_time_dequeue.append(response_time)
                x = router.put_chuncked(
                    task_id= task_id,
                    chunks=chunks,
                    headers={**headers,"Peer-Id":peer_id}
                )
                if x.is_ok:
                    success.append((task_id, peer_id,x))
                else:
                    failed.append((task_id,peer_id, x))

            if len(success) ==0:
                detail = "\n".join([ "{}: {} failed - {}".format(task_id,peer_id, str(r.unwrap_err())) for (task_id, peer_id, r) in failed])
                self.__log.error({
                    "event":"PUT.CHUNKED.FAILED",
                    "bucket_id":_bucket_id,
                    "key":_key,
                    "detail":detail,
                })
                return Err(Exception("Put failed: {}".format(detail)))
            replicas = list(map(lambda x : x[1],success))

         
            response_time = T.time() - start_time
            self.__log.info({
                "event":"PUT.CHUNKED",
                "bucket_id":_bucket_id,
                "key":_key,
                "checksum":checksum,
                "size":size,
                "metadata_service_time": put_metadata_response.service_time,
                "metadata_response_time": put_metadata_rt,
                "replicas":replicas,
                "response_time":response_time,
            })
            return Ok(InterfaceX.PutChunkedResponse(
                bucket_id=_bucket_id,
                key = _key,
                replicas=replicas,
                response_time=response_time,
                size=size,
                throughput=float(size*len(replicas))/float(response_time),
            ))
                # return x
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
        finally:
            with self.__lock:
                self.__put_counter+=1

    def put(self,
            bucket_id:str,
            value:bytes,
            key:str="",
            ball_id:str ="",
            tags:Dict[str,str]={},
            chunk_size:str="1MB",
            timeout:int = 120,
            disabled:bool=False,
            content_type:str = "application/octet-stream",
            replication_factor:int  = 1,
            headers:Dict[str,str]={}
    )-> Result[InterfaceX.PutChunkedResponse,Exception]:
        """
        Stores data in the specified bucket with optional parameters.

        Args:
            bucket_id (str): The ID of the bucket where data will be stored.
            value (bytes): The data to be stored.
            key (str): The key for the stored data. Defaults to an empty string.
            ball_id (str): An identifier for the ball. Defaults to an empty string.
            tags (Dict[str, str]): Metadata tags associated with the data. Defaults to an empty dictionary.
            chunk_size (str): The size of each chunk. Defaults to "1MB".
            timeout (int): The timeout for the operation in seconds. Defaults to 120.
            disabled (bool): If True, the operation is disabled. Defaults to False.
            content_type (str): The MIME type of the content. Defaults to "application/octet-stream".
            replication_factor (int): The number of replications for the data. Defaults to 1.
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.

        Returns:
            Result[InterfaceX.PutChunkedResponse, Exception]: The result of the put operation, including either a successful response or an exception.
        """
        _key = Utils.sanitize_str(x=key)
        _ball_id = Utils.sanitize_str(x=ball_id)
        _bucket_id = Utils.sanitize_str(x=bucket_id)

        if _key == "" and bucket_id =="" :
            return Err(Exception("<key> and <bucket_id> are empty."))
        chunks = Utils.to_gen_bytes(data=value, chunk_size=chunk_size)
        return self.put_chunked(
            key=_key,
            chunks=chunks,
            tags=tags,
            ball_id = _key if _ball_id == "" else _ball_id,
            bucket_id=self.__bucket_id if _bucket_id == "" else _bucket_id,
            timeout=timeout,
            disabled=disabled,
            headers=headers,
            content_type=content_type,
            replication_factor = replication_factor
        )
    def put_async(self,
            bucket_id:str,
            value:bytes,
            key:str="",
            ball_id:str ="",
            tags:Dict[str,str]={},
            chunk_size:str="1MB",
            timeout:int = 120,
            disabled:bool=False,
            content_type:str = "application/octet-stream",
            replication_factor:int = 1,
            headers:Dict[str,str]={}
    )-> Awaitable[Result[InterfaceX.PutChunkedResponse,Exception]]:
        """put_async is an async version of put"""
        _key = Utils.sanitize_str(x=key)
        _ball_id = Utils.sanitize_str(x=ball_id)
        _bucket_id = Utils.sanitize_str(x=bucket_id)

        if _key == "" and bucket_id =="" :
            return Err(Exception("<key> and <bucket_id> are empty."))

        return self.__thread_pool.submit(self.put_chunked,
                                         key=_key,
                                         chunks=Utils.to_gen_bytes(data=value, chunk_size=chunk_size),
                                         tags=tags,
                                         ball_id = _key if _ball_id == "" else _ball_id,
                                         bucket_id=self.__bucket_id if _bucket_id == "" else _bucket_id,
                                         timeout=timeout,
                                         disabled=disabled,
                                         content_type = content_type,
                                         replication_factor = replication_factor,
                                         headers=headers
                                         )
    def put_file_chunked(self,
                         path:str,
                         chunk_size:str = "1MB",
                         bucket_id:str="",
                         key:str="",
                         ball_id:str="",
                        #  peer_id:Option[str]=NONE,
                         tags:Dict[str,str]={},
                         timeout:int=30,
                         disabled:bool=False,
                         content_type:str = "application/octet-stream",
                         replication_factor:int = 1,
                         headers:Dict[str,str] = {}
    )->Result[InterfaceX.PutChunkedResponse,Exception]:
        """Put a file from <path> in chunks"""
        try:
            start_time = T.time()
            file_chunks = Utils.file_to_chunks_gen(path=path, chunk_size=chunk_size);
            (checksum,size) = XoloUtils.sha256_file(path=path)
            # bucket_id 
            _key     = Utils.sanitize_str(key if (not key =="")  else checksum)
            fullname,filename,extension = Utils.extract_path_info(path=path)
            tags  = {**tags,
                      "bucket_relative_path":tags.get("bucket_relative_path",fullname),
                      "fullname":fullname,
                      "filename":filename,
                      "extension":extension,
            }

            with self.__lock:
                _peers = list(map(lambda x: x.router_id,self.__routers))
                # ______________________________________________________
                router    = self.__lb(
                    operation_type = "PUT",
                    algorithm      = self.__lb_algorithm,
                    key            = _key,
                    peers          = _peers,
                    size           = size
                )
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            ball_id = key if ball_id =="" else ball_id

            put_metadata_start_time = T.time()
            put_metadata_result = router.put_metadata(
                bucket_id    = _bucket_id,
                key          = _key, 
                size         = size, 
                checksum     = checksum,
                tags         = tags,
                producer_id  = self.client_id,
                content_type = content_type,
                ball_id      = ball_id,
                timeout      = timeout,
                is_disabled   = disabled,
                headers= headers,
                replication_factor= replication_factor,
            )
            if put_metadata_result.is_err:
                raise put_metadata_result.unwrap_err()
            



            put_metadata_response = put_metadata_result.unwrap()
            put_metadata_rt = T.time() - put_metadata_start_time
        

            datas = tee(file_chunks, len(put_metadata_response.replicas))
            failed = []
            success = []
            for task_id,peer_id,chunks in zip(put_metadata_response.tasks_ids, put_metadata_response.replicas, datas):
            # self.__put_response_time_dequeue.append(response_time)
                x = router.put_chuncked(
                    task_id=task_id,
                    chunks=chunks,
                    headers={**headers,"Peer-Id":peer_id}
                )
                if x.is_ok:
                    success.append((task_id, peer_id,x))
                else:
                    failed.append((task_id,peer_id, x))

            if len(success) ==0:
                detail = "\n".join([ "{}: {} failed - {}".format(task_id,peer_id, str(r.unwrap_err())) for (task_id, peer_id, r) in failed])
                self.__log.error({
                    "event":"PUT.CHUNKED.FAILED",
                    "bucket_id":_bucket_id,
                    "key":_key,
                    "detail":detail,
                })
                return Err(Exception("Put failed: {}".format(detail)))
            replicas = list(map(lambda x : x[1],success))

         
            response_time = T.time() - start_time
            self.__log.info({
                "event":"PUT.FILE.CHUNKED",
                "bucket_id":_bucket_id,
                "key":_key,
                "checksum":checksum,
                "size":size,
                "metadata_service_time": put_metadata_response.service_time,
                "metadata_response_time": put_metadata_rt,
                "replicas":replicas,
                "response_time":response_time,
            })
            return Ok(InterfaceX.PutChunkedResponse(
                bucket_id=_bucket_id,
                key = _key,
                replicas=replicas,
                response_time=response_time,
                size=size,
                throughput=float(size*len(replicas))/float(response_time),
            ))
    
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
        finally:
            with self.__lock:
                self.__put_counter+=1

    def put_ndarray(self, key:str, ndarray:npt.NDArray,tags:Dict[str,str],bucket_id:str="",timeout:int = 120,headers:Dict[str,str]={})->Result[InterfaceX.PutResponse,Exception]:
        try:
            value:bytes = ndarray.tobytes()
            dtype       = str(ndarray.dtype)
            shape_str   = str(ndarray.shape)
            return self.put(key=key, value=value,tags={**tags,"dtype":dtype,"shape":shape_str },bucket_id=bucket_id,timeout=timeout, headers=headers)
        except R.exceptions.HTTPError as e:

            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)

    def put_ndarray_async(self, key:str, ndarray:npt.NDArray,tags:Dict[str,str],bucket_id:str="",timeout:int = 120,headers:Dict[str,str]={})->Awaitable[Result[InterfaceX.PutResponse,Exception]]:
        try:
            value:bytes = ndarray.tobytes()
            dtype       = str(ndarray.dtype)
            shape_str   = str(ndarray.shape)
            return self.put_async(key=key, value=value,tags={**tags,"dtype":dtype,"shape":shape_str },bucket_id=bucket_id,timeout=timeout, headers=headers)
        except R.exceptions.HTTPError as e:

            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
    def put_chunks(self,
                   key:str,
                   chunks:Chunks,
                   tags:Dict[str,str],
                   bucket_id:str="",
                   timeout:int = 120,
                   update:bool = True,
                   headers:Dict[str,str]={}
    )->Generator[Result[InterfaceX.PutResponse,Exception],None,None]:
        
        try:
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            if update:
                self.delete_by_ball_id(ball_id=key,bucket_id=_bucket_id, headers=headers)
            
            futures:List[Awaitable[Result[InterfaceX.PutResponse,Exception]]] = []
            
            for i,chunk in enumerate(chunks.iter()):
                fut = self.put(
                    value=chunk.data,
                    tags={**tags, **chunk.metadata, "index": str(chunk.index), "checksum":chunk.checksum},
                    key=chunk.chunk_id,
                    ball_id=key,
                    bucket_id=_bucket_id,
                    timeout=timeout,
                    # peer_id=peer_id
                )
                futures.append(fut)
            
            for result in as_completed(futures):
                res = result.result()
                yield res

        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)

    def put_file(self,path:str, bucket_id:str= "", update= True, timeout:int = 120, source_folder:str= "",tags={},headers:Dict[str,str]={})->Result[InterfaceX.PutResponse,Exception]:
        """Read a file from disk at <path> and transmit over the network to the Mictlan"""
        _bucket_id = self.__bucket_id if bucket_id=="" else bucket_id
        key = Utils.sanitize_str(key)
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
                    
                    return self.put(
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
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
    

    def put_folder(self,source_path:str,bucket_id="",update:bool = True,headers:Dict[str,str]={}) -> Generator[InterfaceX.PutResponse, None,None]:
        """Read a source folder then transmit the files one by one and return a generator of responses."""
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

    def put_folder_async(self,source_path:str,bucket_id="",update:bool = True,headers:Dict[str,str]={}) -> Generator[InterfaceX.PutResponse, None,None]:
        """Same as put_folder but in runs in a threadpool not block the main thread"""
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        if not os.path.exists(source_path):
            return Err(Exception("{} does not exists".format(source_path)))
        
        failed_operations = []
        _start_time = T.time()
        files_counter = 0
        futures:List[Awaitable[Result[InterfaceX.PutResponse,Exception]]] = []

        for (root,_, filenames) in os.walk(source_path):
            for filename in filenames:
                start_time = T.time()
                path = "{}/{}".format(root,filename)
                
           
                put_file_future = self.__thread_pool.submit(self.put_file, 
                    path=path,
                    bucket_id=_bucket_id,
                    update=update,
                    source_folder=source_path,
                    headers=headers
                )
                futures.append(put_file_future)
          
                    
        

        for fut in as_completed(futures):
            put_file_result:Result[InterfaceX.PutResponse,Exception] = fut.result()
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
                response_time = T.time() - start_time
                self.__log.info({
                    "event":"PUT_FILE",
                    "bucket_id":_bucket_id,
                    "key":response.key,
                    "peer_id":response.node_id, 
                    # "foler_p"
                    "path":path,
                    "response_time":response_time
                })
                files_counter+=1
                yield response
            

        response_time = T.time() - _start_time
        self.__log.info({
            "event":"PUT_FOLDER_ASYNC",
            "bucket_id":bucket_id,
            "folder_path":source_path,
            "response_time":response_time,
            "files_counter":files_counter,
            "failed_puts":len(failed_operations),
        })
    # @deprecated("Use put_chunked instead, it has stream powers!")
    def put_bytes(self,
              value:bytes,
              tags:Dict[str,str]={},
              key:str="",
              ball_id:str="",
              bucket_id:str="",
              content_type="application/octet-stream",
              replication_factor:int= 1,
              timeout:int = 120,
              disabled:bool = False,
              headers:Dict[str,str]={}
    )->Result[InterfaceX.PutResponse,Exception]:
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
            key  = key if (not key =="") else checksum
            key  = Utils.sanitize_str(key)
            # if the ball_id is empty then use the key as ball_id.
            ball_id = key if ball_id == "" else ball_id
            # 
            size    = len(value)
            router    = self.__lb(
                operation_type = "PUT",
                algorithm      = self.__lb_algorithm,
                key            = key,
                peers          = self.__routers,
                size           = size
            )
     
            # content_type = "application/octet-stream"

                
            
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            put_metadata_start_time = T.time()
            put_metadata_result = router.put_metadata(
                key          = key, 
                size         = size, 
                checksum     = checksum,
                tags         = tags,
                producer_id  = self.client_id,
                content_type = content_type,
                ball_id      = ball_id,
                bucket_id    = _bucket_id,
                timeout      = timeout,
                is_disabled   = disabled,
                headers=headers,
                replication_factor=replication_factor
            )

            if put_metadata_result.is_err:
                raise put_metadata_result.unwrap_err()
            
            put_metadata_response:InterfaceX.PutMetadataResponse = put_metadata_result.unwrap()
            put_metadata_rt = T.time() - put_metadata_start_time
            failed = []
            success = []
            for task_id,peer_id in zip(put_metadata_response.tasks_ids,put_metadata_response.replicas):
                # if task_id == "0":
                #     response_time = T.time() - start_time
                #     res = InterfaceX.PutResponse(
                #         key           = key,
                #         response_time = response_time,
                #         throughput    = float(size) / float(response_time),
                #         node_id       = put_metadata_response.node_id
                #     )
                #     return Ok(res)
                _headers = {**headers,"Peer-Id":peer_id}
                put_response = router.put_data(task_id= task_id, key= key, value= value, content_type=content_type,timeout=timeout,headers=_headers)
                if put_response.is_err:
                    failed.append((task_id,peer_id,put_response))
                else:
                    success.append((task_id,peer_id,put_response))
                    # raise put_response.unwrap_err()
            if len(success) ==0:
                detail = "\n".join([ "{}: {} failed - {}".format(task_id,peer_id, str(r.unwrap_err())) for (task_id, peer_id, r) in failed])
                return Err(Exception("Put failed: {}".format(detail)))

            response_time = T.time() - start_time
            replicas = list(map(lambda x: x[1], success))
            self.__log.info({
                "event":"PUT",
                "bucket_id":bucket_id,
                "key":key,
                "checksum":checksum,
                "size":size,
                "metadata_service_time": put_metadata_response.service_time,
                "metadata_response_time": put_metadata_rt,
                "replicas":replicas,
                "response_time":response_time,
            })

            
            # _____________________________
            res = InterfaceX.PutResponse(
                key           = key,
                response_time = response_time,
                throughput    = float(size*len(replicas)) / float(response_time),
                replicas= replicas
            )
            return Ok(res)

            
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            with self.__lock:
                self.__put_counter-=1
            return Err(e)

    
    # ===============END - PUTS==============================
        
            
    def get_default_router(self)->InterfaceX.Router:
        return self.__routers[0]
    def get_router_by_id(self, router_id:str)->Option[InterfaceX.Router]:
        if len(self.__routers) == 0:
            return NONE
        x = next(filter(lambda x: x.router_id == router_id,self.__routers),self.__routers[0])
        return Some(x)
    # Return the unavailible peers

            

    def __global_operation_counter(self):
        return self.__get_counter + self.__put_counter

    def __lb(self,operation_type:str,algorithm:str="ROUND_ROBIN",key:str="",size:int= 0,peers:List[str]=[])->InterfaceX.Router:
        try:
            # filtered_peers = list(filter(lambda x: x.peer_id in peers,self.__peers))
            filtered_routers = self.__routers.copy()
            if len(filtered_routers) == 1:
                return filtered_routers[0]
            
            if algorithm =="ROUND_ROBIN":
                return self.__lb_rb(operation_type,peers=filtered_routers)
            elif algorithm == "HASH":
                return self.__lb_hash(operation_type,key=key, peers=filtered_routers)
            elif algorithm == "PSEUDORANDOM":
                return self.__lb_pseudo_random(operation_type,key=key, peers=filtered_routers)
            elif algorithm == "2CHOICES":
                return self.__lb__two_choices(operation_type,key=key, peers=filtered_routers)
            # elif algorithm == "SORT_UF":
            #     return self.__lb_sort_uf(operation_type = operation_type , key= key,size=size, peers=filtered_peers)
            # elif algorithm == "2CHOICES_UF":
            #     return self.__lb_2choices_uf(operation_type = operation_type , key= key,size=size, peers=filtered_peers)
            else:
                return self.__lb_rb(operation_type,peers=filtered_routers)
        except Exception as e:
            self.__log.error("LB_ERROR "+str(e))

        
    def __lb__two_choices(self,operation_type:str,peers:List[InterfaceX.Router])-> InterfaceX.Router: 
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
    def __lb_rb(self,operation_type:str,peers:List[InterfaceX.Router]=[])->InterfaceX.Router:
        x = self.__global_operation_counter()
        return peers[x % len(peers)]
    
    def __lb_hash(self,key:str, peers:List[InterfaceX.Router]=[])->InterfaceX.Router:
        return peers[hash(key) % len(peers)]

    def __lb_pseudo_random(self,key:str, peers:List[InterfaceX.Router]=[])->InterfaceX.Router:
        return peers[np.random.randint(0,len(peers))]


    def shutdown(self):
        try:
            self.__thread_pool.shutdown(wait=False)
            if self.__daemon:
                self.enable_daemon=False
                # self.__thread.join(timeout=30)
            self.__log.debug("SHUTDOWN {}".format(self.client_id))
        except Exception as e:
            self.__log.error({
                "event":"SHUTDOWN.FAILED",
                "error":str(e)
            })

    # GET
    def get_decrypt(self,
            bucket_id:str,
            secret_key:bytes,
            secret_header:Option[bytes] = NONE,
            key:str="",
            chunk_size:str="1MB",
            timeout:int = 120,
            headers:Dict[str,str]={}
    ):
        try:
            start_time = T.time()
            x = self.get(key=key,bucket_id=bucket_id, timeout=timeout, headers=headers, chunk_size=chunk_size)
            if x.is_err:
                return x
            
            get_response = x.unwrap()
            value  = get_response.value
            decrypted_data = XoloUtils.decrypt_aes(key=secret_key,data=value, header=secret_header)
            if decrypted_data.is_err:
                return Err(decrypted_data.unwrap_err())
            
            self.__log.info({
                "event":"GET.DECRYPTED",
                "bucket_id":bucket_id,
                "key":key,
                "response_time":T.time() - start_time
            })
            return Ok(InterfaceX.GetBytesResponse(
                value=decrypted_data.unwrap(),
                metadata=get_response.metadata,
                response_time= get_response.response_time
            ))

            
        except Exception as e:
            return Err(e)
    def get_bucket_metadata(self,bucket_id:str, router:InterfaceX.Router=NONE, timeout:int = 120,headers:Dict[str,str]={})->Result[InterfaceX.GetRouterBucketMetadataResponse,Exception]: 
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
                

        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)

    def get_bucket_metadata_async(self,bucket_id:str, router:Option[InterfaceX.Router]=NONE, timeout:int = 120,headers:Dict[str,str]={})->Awaitable[Result[InterfaceX.GetRouterBucketMetadataResponse, Exception]]:
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
            x     = self.__thread_pool.submit(self.get_bucket_metadata, bucket_id=_bucket_id,timeout=timeout, router= _router,headers=headers)
            # service_time = T.time() - start_time
            return x

        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)



    def get_with_retry(
            self, 
                               key:str,
                               chunk_size:str="1MB",
                               max_retries:int = 10,
                               delay:float =1,
                               max_delay:float = 2, 
                               backoff:float = 1,
                               jitter:float = 0,
                               bucket_id:str="",
                               timeout:int= 120,
                               headers:Dict[str,str]={},
    )->Result[InterfaceX.GetBytesResponse,Exception]:
        # start_time = T.time()
        try:
            
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            get_result = retry_call(
                f = self.__get_and_raise_exception, 
                fkwargs={
                    "key":key,
                    "timeout":timeout,
                    "chunk_size":chunk_size,
                    "bucket_id":_bucket_id,
                    "headers":headers,
                } ,
                delay = delay,
                tries = max_retries,
                max_delay = max_delay,
                backoff =backoff,
                jitter = jitter,
                logger= self.__log,
            )

            return Ok(get_result)
                
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
        
    def get_ndarray_with_retry(self, 
                               key:str,
                               max_retries:int = 10,
                               delay:float =1,
                               max_delay:float = 2, 
                               backoff:float = 1,
                               jitter:float = 0,
                               bucket_id:str="",
                               timeout:int= 120,
                               headers:Dict[str,str]={}
    )->Awaitable[Result[InterfaceX.GetNDArrayResponse,Exception]]:
        start_time = T.time()
        try:
            
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            def __inner()->Result[InterfaceX.GetNDArrayResponse,Exception]:
                get_result =  self.get_with_retry(key=key,timeout=timeout,bucket_id=_bucket_id,headers=headers, max_retries=max_retries, delay=delay, max_delay=max_delay,backoff=backoff,jitter=jitter)
                if get_result.is_ok:
                    get_response = get_result.unwrap()
                    metadata     = get_response.metadata
                    shape        = eval(metadata.tags["shape"])
                    dtype        = metadata.tags["dtype"]
                    ndarray      = np.frombuffer(get_response.value,dtype=dtype).reshape(shape)
                    response_time = T.time() - start_time
                    return Ok(InterfaceX.GetNDArrayResponse(value=ndarray, metadata=metadata, response_time=response_time))
                else:
                    return get_result
                
            return self.__thread_pool.submit(__inner)

        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)


    def get_ndarray(self, key:str,bucket_id:str="",timeout:int= 120,headers:Dict[str,str]={})->Awaitable[Result[InterfaceX.GetNDArrayResponse,Exception]]:
        start_time = T.time()
        try:
            
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            key = Utils.sanitize_str(key)
            def __inner()->Result[InterfaceX.GetNDArrayResponse,Exception]:
                get_result =  self.get(key=key,timeout=timeout,bucket_id=_bucket_id,headers=headers)
                if get_result.is_ok:
                    get_response = get_result.unwrap()
                    metadata     = get_response.metadata
                    shape        = eval(metadata.tags["shape"])
                    dtype        = metadata.tags["dtype"]
                    ndarray      = np.frombuffer(get_response.value,dtype=dtype).reshape(shape)
                    response_time = T.time() - start_time
                    return Ok(InterfaceX.GetNDArrayResponse(value=ndarray, metadata=metadata, response_time=response_time))
                else:
                    return get_result
                
            # get_future_result =  self.get(key=key,timeout=timeout)
            return self.__thread_pool.submit(__inner)
                # get_result.add_done_callback(Client.fx)

        except R.exceptions.HTTPError as e:

            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)


    def get_metadata_async(self,key:str,bucket_id:str="",router:Option[InterfaceX.Router]=NONE ,timeout:int=120,headers:Dict[str,str]={}) -> Awaitable[Result[InterfaceX.GetMetadataResponse, Exception]] :
        try:
            if router.is_none:
                _router = self.__lb(
                        operation_type="GET",
                        algorithm=self.__lb_algorithm,
                        key=key,
                        size=0,
                        peers=list(map(lambda x: x.router_id,self.__routers))
                )
            else:
                _router = router.unwrap()
            # _peer.get_metadata()
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            key = Utils.sanitize_str(key)
            return self.__thread_pool.submit(_router.get_metadata, key = key, timeout = timeout,bucket_id=_bucket_id,headers=headers)
        
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
 
    def get_metadata(self,key:str,bucket_id:str="",router:Option[InterfaceX.Router]=NONE ,timeout:int=120,headers:Dict[str,str]={}) -> Result[InterfaceX.GetMetadataResponse, Exception]:
        try:
            if router.is_none:
                _router = self.__lb(
                        operation_type="GET",
                        algorithm=self.__lb_algorithm,
                        key=key,
                        size=0,
                        peers=list(map(lambda x: x.router_id,self.__routers))
                )
            else:
                _router = router.unwrap()
            # _peer.get_metadata()
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            key = Utils.sanitize_str(key)
            return _router.get_metadata(key = key, timeout = timeout,bucket_id=_bucket_id,headers=headers)
        
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)


    def get_to_file_with_retry(
        self,
        key:str,
        bucket_id:str="",
        filename:str="",
        chunk_size:str="1MB",
        output_path:str="/mictlanx/data",
        max_retries:int = 10,
        delay:float =1,
        max_delay:float = 2, 
        backoff:float = 1,
        jitter:float = 0,
        timeout:int= 120,
        headers:Dict[str,str]={},
    )->Result[InterfaceX.GetToFileResponse,Exception]:
        try:
            
            _bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            get_result = retry_call(
                f = self.__get_to_file_and_raise, 
                fkwargs={
                    "key":key,
                    "timeout":timeout,
                    "chunk_size":chunk_size,
                    "bucket_id":_bucket_id,
                    "headers":headers,
                    "output_path":output_path,
                    "filename":filename
                } ,
                delay = delay,
                tries = max_retries,
                max_delay = max_delay,
                backoff =backoff,
                jitter = jitter,
                logger= self.__log,
            )

            return Ok(get_result)
                
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)

    def get_to_file(self,
                    key:str,
                    bucket_id:str,
                    filename:str="",
                    chunk_size:str="1MB",
                    output_path:str="/mictlanx/data",
                    timeout:int = 120,
                    headers:Dict[str,str]={}
    )->Result[InterfaceX.GetToFileResponse,Exception]:
        """
        Retrieves data from the specified bucket using the given key and saves it to a file.

        Args:
            key (str): The key for the data to be retrieved.
            bucket_id (str): The ID of the bucket from which data will be retrieved.
            filename (str): The name of the file to save the retrieved data. Defaults to an empty string.
            chunk_size (str): The size of each chunk to retrieve. Defaults to "1MB".
            output_path (str): The path where the file will be saved. Defaults to "/mictlanx/data".
            timeout (int): The timeout for the operation in seconds. Defaults to 120 seconds.
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.

        Returns:
            Result[InterfaceX.GetToFileResponse, Exception]: The result of the get operation, including either a successful response or an exception.
        """
        try:
            start_time      = T.time()
            _key            = Utils.sanitize_str(x=key)
            _bucket_id      = Utils.sanitize_str(x=bucket_id)
            _bucket_id      = self.__bucket_id if _bucket_id =="" else _bucket_id
                
            routers_ids     = list(map(lambda x:x.router_id, self.__routers))
            selected_router   = self.__lb(operation_type="GET", algorithm=self.__lb_algorithm, key=key, size=0,  peers=routers_ids )
            metadata_result = selected_router.get_metadata(bucket_id=_bucket_id, key=_key,timeout=timeout)


            if metadata_result.is_err:
                raise Exception("{}@{} metadata not found.".format(_bucket_id,_key))
            
            metadata             = metadata_result.unwrap()
            
            bucket_relative_folder_path = os.path.dirname(metadata.metadata.tags.get("bucket_relative_path",output_path))
            
            # bucket_relative_path = output_path if bucket_relative_folder_path == "" else bucket_relative_folder_path
            bucket_relative_path = os.path.join(output_path, bucket_relative_folder_path)
            # output_path if bucket_relative_folder_path == "" else bucket_relative_folder_path
            
            _filename = metadata.metadata.tags.get("fullname","") if filename == "" else filename
            if _filename == "":
                raise Exception("You must set a <filename> use the kwargs parameter filename=\"myfile.pdf\"")
            
            maybe_local_path = selected_router.get_to_file(
                bucket_id=_bucket_id,
                key=_key,
                chunk_size=chunk_size,
                sink_folder_path=bucket_relative_path,
                filename=_filename,
                # metadata.metadata.tags.get("fullname",filename),
                timeout=timeout,
                headers=headers
            )
            if maybe_local_path.is_err:
                return maybe_local_path
            local_path = maybe_local_path.unwrap()
            rt = T.time()- start_time
            self.__log.info({
                "event":"GET.TO.FILE",
                "bucket_id":bucket_id,
                "key":key,
                "chunk_size":chunk_size,
                "path":local_path,
                "response_time":rt
            })
            return Ok(InterfaceX.GetToFileResponse(
                path= local_path,
                response_time=rt ,
                metadata=metadata,
                peer_id=metadata.peer_id
            ))

        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
    def __get_to_file_and_raise(self,
                    key:str,
                    bucket_id:str,
                    filename:str="",
                    chunk_size:str="1MB",
                    output_path:str="/mictlanx/data",
                    timeout:int = 120,
                    headers:Dict[str,str]={}
                                ):
        res = self.get_to_file(key=key,bucket_id=bucket_id,filename=filename,chunk_size=chunk_size,output_path=output_path, timeout=timeout,headers=headers)
        if res.is_ok:
            return res.unwrap()
        else:
            raise res.unwrap_err()


    def get_async(self,key:str,bucket_id:str="",timeout:int = 120,headers:Dict[str,str]={},chunk_size:str="1MB")->Awaitable[Result[InterfaceX.GetBytesResponse,Exception]]:

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id

        if _key == "" and bucket_id =="" :
            return Err(Exception("<key> and <bucket_id> are empty."))
        
        x = self.__thread_pool.submit(
            self.get, key = _key,timeout=timeout,bucket_id=_bucket_id,headers=headers,chunk_size = chunk_size
        )
        return x

    def get(self,
            key:str,
            bucket_id:str="",
            timeout:int=120,
            chunk_size:str="1MB",
            headers:Dict[str,str]={}
    )->Result[InterfaceX.GetBytesResponse,Exception]:
        """
        Retrieves data from the specified bucket using the given key.

        Args:
            key (str): The key for the data to be retrieved.
            bucket_id (str): The ID of the bucket from which data will be retrieved. Defaults to an empty string.
            timeout (int): The timeout for the operation in seconds. Defaults to 120.
            chunk_size (str): The size of each chunk to retrieve. Defaults to "1MB".
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.

        Returns:
            Result[InterfaceX.GetBytesResponse, Exception]: The result of the get operation, including either a successful response or an exception.
        """
        try:
            _chunk_size= HF.parse_size(chunk_size)
            start_time = T.time()
            
            bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            selected_router = self.__lb(
                operation_type = "GET",
                algorithm      = self.__lb_algorithm,
                key            = key,
                size           = 0,
                peers          = list(map(lambda x: x.router_id,self.__routers))
            )
            # _____________________________________________________________________________________________________________________
            get_metadata_result:Result[InterfaceX.GetMetadataResponse,Exception] = selected_router.get_metadata(bucket_id=bucket_id,key=key,timeout=timeout, headers=headers)
            # self.__get_metadata(bucket_id=bucket_id,key= key, peer=selected_peer,timeout=timeout)
            # .result()
            if get_metadata_result.is_err:
                raise Exception(str(get_metadata_result.unwrap_err()))
            metadata_response = get_metadata_result.unwrap()
            # metadata_response.node_id
            metadata_service_time = T.time() - start_time
            # _________________________________________________________________________
            headers["Peer-Id"] = metadata_response.peer_id
            headers["Local-Peer-Id"] = metadata_response.local_peer_id
            result = selected_router.get_streaming(bucket_id=bucket_id,key=key,timeout=timeout,headers=headers,)
            if result.is_err:
                raise result.unwrap_err()

            response = result.unwrap()
            response_time = T.time() - start_time
            value = bytearray()
            for chunk in response.iter_content(chunk_size=_chunk_size):
                value.extend(chunk)
            # 
            self.__log.info(
                {
                    "event":"GET",
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":metadata_response.metadata.size, 
                    "peer_id":metadata_response.peer_id,
                    "local_peer_id":metadata_response.local_peer_id,
                    "response_time":response_time,
                    "metadata_service_time":metadata_service_time,
                }
            )
            return Ok(InterfaceX.GetBytesResponse(value=bytes(value),metadata=metadata_response.metadata,response_time=response_time))


        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)

    def __get_and_raise_exception(self,key:str,bucket_id:str="",timeout:int=120,chunk_size:str="1MB",headers:Dict[str,str]={})->InterfaceX.GetBytesResponse:
        try:
            _chunk_size= HF.parse_size(chunk_size)
            start_time = T.time()
            
            bucket_id = self.__bucket_id if bucket_id =="" else bucket_id
            key = Utils.sanitize_str(key)
            selected_peer = self.__lb(
                operation_type = "GET",
                algorithm      = self.__lb_algorithm,
                key            = key,
                size           = 0,
                peers          = list(map(lambda x: x.router_id,self.__routers))
            )
            # _____________________________________________________________________________________________________________________
            get_metadata_result:Result[InterfaceX.GetMetadataResponse,Exception] = selected_peer.get_metadata(bucket_id=bucket_id,key=key,timeout=timeout, headers=headers)
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
                    "peer_id":metadata_response.peer_id,
                }
            )
            return InterfaceX.GetBytesResponse(value=bytes(value),metadata=metadata_response.metadata,response_time=response_time)


        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            raise e
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            raise e


    @deprecated("Same as get_and_merge")
    def get_and_merge_ndarray(self,key:str,bucket_id:str="",timeout:int= 120,headers:Dict[str,str]={}) -> Awaitable[Result[InterfaceX.GetNDArrayResponse,Exception]]:

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        return self.__thread_pool.submit(self.__get_and_merge_ndarray, key = _key,timeout=timeout,bucket_id=_bucket_id,headers=headers)
    
    def __get_and_merge_ndarray(self,key:str,bucket_id:str="",timeout:int=120,headers:Dict[str,str]={})->Result[InterfaceX.GetNDArrayResponse,Exception] :
        try:
            start_time = T.time()
            res:Result[InterfaceX.GetBytesResponse,Exception] = self.__get_and_merge(bucket_id=bucket_id,key=key,timeout=timeout,headers=headers)
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
                return Ok(InterfaceX.GetNDArrayResponse(value=ndarray, metadata=response.metadata, response_time=response_time))
            else:

                return res
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
        
    def __get_metadata_peers_async(self,key:str,bucket_id:str="",timeout:int = 120,headers:Dict[str,str]={})->Generator[List[InterfaceX.Metadata],None,None]:
        for peer in self.__routers:
            metadata_result = peer.get_chunks_metadata(key=key,bucket_id=bucket_id,timeout=timeout,headers=headers)
            if metadata_result.is_ok:
                yield metadata_result.unwrap()



    def __get_metadata_valid_index(self,key:str,bucket_id:str="",timeout:int = 120,headers:Dict[str,str]={})->Generator[InterfaceX.Metadata,None,None]: 
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

    def get_and_merge_with_num_chunks(self, key:str,num_chunks:int,bucket_id:str = "",timeout:int = 120, max_retries:Option[int]=NONE,headers:Dict[str,str]={})->Awaitable[Result[InterfaceX.GetBytesResponse,Exception]]:

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
    

    def __get_chunks_and_fails(self,bucket_id:str,chunks_ids:List[str],timeout:int=120,headers:Dict[str,str]={})->Tuple[List[InterfaceX.GetBytesResponse], List[str]]:
        max_iter = len(self.__routers)*2
        xs:List[InterfaceX.GetBytesResponse] = []
        failed_chunk_keys = []
        for chunk_key in chunks_ids:
            i = 0 
            res:Result[InterfaceX.GetBytesResponse,Exception] = self.get(bucket_id=bucket_id,key=chunk_key,timeout=timeout,headers=headers)
            # ______________________________________
            while res.is_err and i < max_iter:
                res:Result[InterfaceX.GetBytesResponse,Exception] = self.get(bucket_id=bucket_id,key=chunk_key,timeout=timeout)
                i+=1
                if i>=max_iter and res.is_err:
                    failed_chunk_keys.append(chunk_key)
            if res.is_ok:
                x = res.unwrap()
                xs.append(x)
        return (xs, failed_chunk_keys)
    
    
    def __get_and_merge_with_num_chunks(self,key:str,num_chunks:int,bucket_id:str ="",timeout:int = 120,max_retries:Option[int]=NONE,headers:Dict[str,str]={})->Result[InterfaceX.GetBytesResponse, Exception]:
        try:
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
                
            chunk_metadata = InterfaceX.Metadata(key=key,size=size,checksum=checksum,tags=tags, content_type=content_type, producer_id=producer_id, ball_id=key)
            return Ok(InterfaceX.GetBytesResponse(value=merged_bytes, metadata=chunk_metadata,response_time=response_time ))

        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)

    @deprecated("This is so complex need a complete refactoring")
    def get_and_merge(self, key:str,bucket_id:str="",timeout:int = 120,headers:Dict[str,str]={})->Awaitable[Result[InterfaceX.GetBytesResponse,Exception]]:

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        return self.__thread_pool.submit(self.__get_and_merge,
                                         key = _key,
                                         bucket_id=_bucket_id,
                                         timeout=timeout,
                                         headers = headers
        )
    
    def __get_and_merge(self,key:str,bucket_id:str="",timeout:int = 120,headers:Dict[str,str]={})->Result[InterfaceX.GetBytesResponse, Exception]:
        try:
            start_time = T.time()
            _bucket_id = self.__bucket_id if bucket_id == "" else bucket_id
            results:List[Awaitable[Result[InterfaceX.GetBytesResponse,Exception]]] = []
            metadatas_gen = list(self.__get_metadata_valid_index(key=key,timeout=timeout,bucket_id=_bucket_id,headers=headers))
            if len(metadatas_gen)==0:
                return Err(Exception("{}/{} not found".format(bucket_id,key)))
            i = 0 
            worker_buffer_size = int(self.__max_workers/2)
            worker_buffer_size = self.__max_workers if worker_buffer_size == 0 else worker_buffer_size
            get_reponses:List[InterfaceX.GetBytesResponse] = []
            for chunk_metadata in metadatas_gen:
                res   = self.get_async(bucket_id=_bucket_id,key=chunk_metadata.key,timeout=timeout,headers=headers)
                results.append(res)
                i += 1
                if i % worker_buffer_size == 0:
                    for chunk_metadata in as_completed(results):
                        result:Result[InterfaceX.GetBytesResponse,Exception] = chunk_metadata.result()
                        if result.is_ok:
                            response:InterfaceX.GetBytesResponse = result.unwrap()
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
                
            chunk_metadata = InterfaceX.Metadata(key=key,size=size,checksum=checksum,tags=tags, content_type=content_type, producer_id=producer_id, ball_id=key)
            return Ok(InterfaceX.GetBytesResponse(value=merged_bytes, metadata=chunk_metadata,response_time=response_time ))

        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)

    def delete_by_ball_id(self,ball_id:str,bucket_id:str="",timeout:int=120,headers:Dict[str,str]={})->Result[InterfaceX.DeleteByBallIdResponse,Exception]:
        _bucket_id = self.__bucket_id if bucket_id == "" else bucket_id
        try:
            del_by_bid_response_global = InterfaceX.DeleteByBallIdResponse(n_deletes=0,ball_id=ball_id)
            for router in self.__routers:
                start_time = T.time()
                result = router.delete_by_ball_id(ball_id=ball_id,bucket_id=_bucket_id,timeout=timeout,headers=headers)
                if result.is_err:
                    raise result.unwrap_err()
                
                del_by_bid_response = result.unwrap()
                del_by_bid_response_global.n_deletes+= del_by_bid_response.n_deletes
                service_time = T.time() - start_time
                self.__log.debug({
                    "event":"DELETE.BY.BALL_ID",
                    "bucket_id":_bucket_id,
                    "ball_id":ball_id,
                    "n_deletes":del_by_bid_response.n_deletes,
                    "response_time":service_time
                })
            return Ok(del_by_bid_response_global)
        
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)


    def delete_bucket(self,bucket_id:str)->Result[str,Exception]:
        """
        Deletes the specified bucket.

        Args:
            bucket_id (str): The ID of the bucket to be deleted.

        Returns:
            Result[str, Exception]: The result of the delete operation, which is a success message or an exception.
        """      
        try:
            n_deleted_objects = 0 
            global_start_time = T.time()
            for metadata in self.get_all_bucket_metadata(bucket_id=bucket_id):
                for ball in metadata.balls:
                    del_result = self.delete(key=ball.key,bucket_id=bucket_id)
                    if del_result.is_ok:
                        n_deleted_objects +=1


            res = InterfaceX.BucketDeleteResponse(
                n_deleted_objects=n_deleted_objects,
                response_time=T.time() - global_start_time
            )

            self.__log.info({
                "event":"DELETE.BUCKET",
                "bucket_id":bucket_id,
                "n_deleted_objects":n_deleted_objects,
                "response_time":res.response_time
            })

            return Ok(res)
        except Exception as e:
            return Err(e)

    def delete_async(self, key:str,bucket_id:str="",timeout:int = 120,headers:Dict[str,str]={})->Awaitable[Result[InterfaceX.DeleteByKeyResponse, Exception]]:
        return self.__thread_pool.submit(
            self.delete,
            key,bucket_id,timeout,headers
        )
    def delete(self, 
               key:str,
               bucket_id:str="",
               timeout:int = 120,
               headers:Dict[str,str]={}
    )->Result[InterfaceX.DeleteByKeyResponse, Exception]:
        """
        Deletes the data associated with the given key from the specified bucket.

        Args:
            key (str): The key for the data to be deleted.
            bucket_id (str): The ID of the bucket from which the data will be deleted. Defaults to an empty string.
            timeout (int): The timeout for the operation in seconds. Defaults to 120 seconds.
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.

        Returns:
            Result[InterfaceX.DeleteByKeyResponse, Exception]: The result of the delete operation, which includes either a successful response or an exception.
        """
      

        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id =="" else _bucket_id
        try:
            failed=[]
            del_res = InterfaceX.DeleteByKeyResponse(n_deletes=0, key=key)
            for router in self.__routers:
                start_time = T.time()
                del_result = router.delete(bucket_id=_bucket_id,key=_key,headers=headers,timeout=timeout)
                if del_result.is_err:
                    response_time = T.time() - start_time
                    # del_res.n_deletes -=1
                    self.__log.error({
                        "event":"DELETE",
                        "bucket_id":_bucket_id,
                        "key":key,
                        "router_id":router.router_id,
                        "response_time":response_time
                    })
                    failed.append(router)
                else:
                    x_response = del_result.unwrap()
                    del_res.n_deletes +=x_response.n_deletes
                    response_time = T.time() - start_time
                    self.__log.info({
                        "event":"DELETE",
                        "bucket_id":_bucket_id,
                        "key":_key,
                        "n_deletes":del_res.n_deletes,
                        "router_id":router.router_id,
                        "response_time":response_time
                    })
                
            return Ok(del_res)
        except R.exceptions.HTTPError as e:
            self.__log_response_error(e)
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return Err(e)
    
    
    def get_all_bucket_metadata(self, bucket_id:str,headers:Dict[str,str ]={},timeout:int = 120)-> Generator[InterfaceX.GetRouterBucketMetadataResponse,None,None]:
        futures = []
        start_time = T.time()
        for router in self.__routers:
            fut = self.get_bucket_metadata_async(bucket_id=bucket_id,router= Some(router),headers=headers,timeout=timeout)
            futures.append(fut)
        for fut in as_completed(futures):
            bucket_metadata_result:Result[InterfaceX.GetRouterBucketMetadataResponse,Exception] = fut.result()
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
        
    def update(self,
        bucket_id:str,
        value:bytes,
        key:str="",
        ball_id:str ="",
        tags:Dict[str,str]={},
        chunk_size:str="1MB",
        timeout:int = 120,
        disabled:bool=False,
        content_type:str = "application/octet-stream",
        replication_factor:int  = 1,
        headers:Dict[str,str]={}
    )->Result[InterfaceX.UpdateResponse,Exception]:
        """
        Updates the data associated with the given key in the specified bucket.

        Args:
            bucket_id (str): The ID of the bucket where the data will be updated.
            value (bytes): The new data to be stored.
            key (str): The key for the data to be updated. Defaults to an empty string.
            ball_id (str): An identifier for the ball. Defaults to an empty string.
            tags (Dict[str, str]): Metadata tags associated with the data. Defaults to an empty dictionary.
            chunk_size (str): The size of each chunk. Defaults to "1MB".
            timeout (int): The timeout for the operation in seconds. Defaults to 120 seconds.
            disabled (bool): If True, the operation is disabled. Defaults to False.
            content_type (str): The MIME type of the content. Defaults to "application/octet-stream".
            replication_factor (int): The number of replications for the data. Defaults to 1.
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.

        Returns:
            Result[InterfaceX.UpdateResponse, Exception]: The result of the update operation, including either a successful response or an exception.
        """   
        try:
            start_time = T.time()
            n_deletes =-1 
            retries = 0
            while n_deletes > 0 or n_deletes == -1 and retries <= 10 :

                deleted_result = self.delete(bucket_id=bucket_id,key=key, timeout=timeout,headers=headers)
                if deleted_result.is_err:
                    return deleted_result
                deleted    = deleted_result.unwrap()
                n_deletes = deleted.n_deletes
                retries +=1 
            put_result  = self.put(bucket_id=bucket_id,key=key, value=value,ball_id=ball_id,tags=tags,chunk_size=chunk_size, timeout=timeout, disabled=disabled, content_type=content_type, replication_factor=replication_factor,headers=headers)
            if put_result.is_err:
                return put_result
            put_response = put_result.unwrap()

            rt = T.time() - start_time
            self.__log.info({
                "event":"UPDATE",
                "updated": n_deletes ==0 and put_result.is_ok,
                "bucket_id":bucket_id,
                "key":key,
                "replicas":put_response.replicas,
                "response_time":rt,
            })
            return Ok(InterfaceX.UpdateResponse(
                updated= n_deletes==0,
                bucket_id=bucket_id,
                key=key,
                replicas=put_response.replicas,
                throughput=float(len(value) * len(put_response.replicas)) / rt,
                response_time=rt
            ))
        except Exception as e:
            return Err(e)
        
    def update_from_file(self,
        bucket_id:str,
        path:str,
        key:str="",
        ball_id:str ="",
        tags:Dict[str,str]={},
        chunk_size:str="1MB",
        timeout:int = 120,
        disabled:bool=False,
        content_type:str = "application/octet-stream",
        replication_factor:int  = 1,
        headers:Dict[str,str]={}
    )->Result[InterfaceX.UpdateResponse,Exception]:
        """
        Updates the data in the specified bucket from a file.

        Args:
            bucket_id (str): The ID of the bucket where the data will be updated.
            path (str): The path to the file containing the new data.
            key (str): The key for the data to be updated. Defaults to an empty string.
            ball_id (str): An identifier for the ball. Defaults to an empty string.
            tags (Dict[str, str]): Metadata tags associated with the data. Defaults to an empty dictionary.
            chunk_size (str): The size of each chunk. Defaults to "1MB".
            timeout (int): The timeout for the operation in seconds. Defaults to 120 seconds.
            disabled (bool): If True, the operation is disabled. Defaults to False.
            content_type (str): The MIME type of the content. Defaults to "application/octet-stream".
            replication_factor (int): The number of replications for the data. Defaults to 1.
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.

        Returns:
            Result[InterfaceX.UpdateResponse, Exception]: The result of the update operation, including either a successful response or an exception.
        """
        try:
            start_time = T.time()
            n_deletes =-1 
            retries = 0
            while n_deletes > 0 or n_deletes == -1 and retries <= 10 :

                deleted_result = self.delete(bucket_id=bucket_id,key=key, timeout=timeout,headers=headers)
                if deleted_result.is_err:
                    return deleted_result
                deleted    = deleted_result.unwrap()
                n_deletes = deleted.n_deletes
                retries +=1 
            put_result  = self.put_file_chunked(bucket_id=bucket_id,key=key, path=path,ball_id=ball_id,tags=tags,chunk_size=chunk_size, timeout=timeout, disabled=disabled, content_type=content_type, replication_factor=replication_factor,headers=headers)
            if put_result.is_err:
                return put_result
            put_response = put_result.unwrap()

            rt = T.time() - start_time
            self.__log.info({
                "event":"UPDATE",
                "updated": n_deletes ==0 and put_result.is_ok,
                "bucket_id":bucket_id,
                "key":key,
                "replicas":put_response.replicas,
                "response_time":rt,
            })
            return Ok(InterfaceX.UpdateResponse(
                updated= n_deletes==0,
                bucket_id=bucket_id,
                key=key,
                replicas=put_response.replicas,
                throughput=float(put_response.size * len(put_response.replicas)) / rt,
                response_time=rt
            ))
        except Exception as e:
            return Err(e)
        
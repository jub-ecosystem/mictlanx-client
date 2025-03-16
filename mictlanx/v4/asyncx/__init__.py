
from typing import List,Dict,Tuple
import itertools
import time as T
import asyncio
import httpx
import mictlanx.interfaces as InterfaceX
from mictlanx.errors import MictlanXError
from mictlanx.caching import CacheFactory
import humanfriendly as HF
from mictlanx.logger import Log
from option import Ok,Some,Result, Err,NONE
from mictlanx.utils.index import Utils
from mictlanx.utils.segmentation import Chunks,Chunk
import os
import mictlanx.errors  as EX
# import ValidationError,NotFoundError,MictlanXError
from mictlanx.v4.asyncx.lb import RouterLoadBalancer
from mictlanx.v4.asyncx.utils import AsyncClientUtils
from xolo.utils.utils import Utils as XoloUtils
from tqdm import tqdm

class AsyncClient():
    """
    The MictlanX Client is an intuitive interface designed to facilitate seamless interactions between users and the MictlanX decentralized storage system. 
    This client provides essential functionalities that enable users to securely store (put) and retrieve (get) data with ease.
    """

    def __init__(
            self,
            client_id:str,
            debug:bool=True,
            max_workers:int = 12,
            log_output_path:str = "/mictlanx/client",
            log_when:str="m",
            log_interval:int = 30,
            routers:List[InterfaceX.Router] = [],
            eviction_policy:str = "LRU",
            capacity_storage:str = "1GB",
            verify:InterfaceX.VerifyType=False
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
            routers (List[InterfaceX.Router]): A list of router interfaces. Defaults to an empty list.  

        """
        self.cache     = CacheFactory.create(eviction_policy=eviction_policy, capacity_storage=HF.parse_size(capacity_storage))
        self.client_id = client_id
        # Peers
        self.__routers = list(map(InterfaceX.AsyncRouter.from_router,routers))
        self.rlb       = RouterLoadBalancer(routers=self.__routers)
        
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
        self.verify = verify
        # PeerID -> PeerStats
        self.__peer_stats:Dict[str, InterfaceX.PeerStats] = {}
        if not os.path.exists(log_output_path):
            os.makedirs(name=log_output_path,mode=0o777,exist_ok=True)
        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers

    
    async def put_chunks(self,bucket_id:str, key:str, chunks:Chunks, chunk_size:str="256kb", rf:int =1, timeout:int=120, max_tries:int=5, max_concurrency:int=2)->Result[bool, EX.MictlanXError]:
        try:
            t1          = T.time()
            _bucket_id  = Utils.sanitize_str(bucket_id)
            _key        = Utils.sanitize_str(key)
            router      = self.rlb.get_router()
            _chunk_size = HF.parse_size(chunk_size)
            # chunks,chunks_2 = itertools.tee(chunks,n=2)
            gen_bytes = chunks.to_generator()
            (checksum,size) = XoloUtils.sha256_stream(gen_bytes)

            num_chunks = len(chunks)
            semaphore = asyncio.Semaphore(max_concurrency)  # ✅ Limit concurrency to 10 uploads at a time
            # progress_bar = tqdm(total=len(value))
            async def upload_chunk(chunk:Chunk, attempt=1)->Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                while attempt <= max_tries:
                    try:
                        async with semaphore:  # ✅ Ensure controlled parallelism
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                client_id=self.client_id,
                                ball_id=key,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={"num_chunks": str(num_chunks), "full_checksum": checksum}
                            )
                        if res.is_ok:
                            return (None,Ok(res.unwrap()))  # ✅ Upload success
                    except Exception as e:
                        self.__log.error({
                            "event":"UPLOAD.CHUNK.FAILED",
                            "detail":str(e)
                        })
                        self.__log.warning(f"Chunk {chunk.chunk_id} failed on attempt {attempt}/{max_tries}. Retrying...")
                        await asyncio.sleep(2 ** attempt)  # ✅ Exponential backoff
                    attempt += 1

                return (chunk,Err(Exception(f"Failed to upload chunk {chunk.chunk_id} after {max_tries} retries.")))

            # ✅ Execute uploads in parallel with retries
            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)

            # ✅ Check if any uploads failed
            failures = [res for res in results if res[1].is_err]
            if len(failures)>0:
                raise EX.PutChunksError(message="")

            self.__log.info({
                "event": "PUT",
                "bucket_id": _bucket_id,
                "key": _key,
                "response_time": T.time() - t1
            })
            return Ok(True)
                
               

        except Exception as e:
            _e = MictlanXError.from_exception(e)
            self.__log.debug({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(e)
    
    async def put_file(self,bucket_id:str, key:str, path:str, chunk_size:str="256kb", rf:int =1, timeout:int=120, max_tries:int=5, max_concurrency:int=2)->Result[bool, EX.MictlanXError]:
        try:
            t1          = T.time()
            _bucket_id  = Utils.sanitize_str(bucket_id)
            _key        = Utils.sanitize_str(key)
            router      = self.rlb.get_router()
            _chunk_size = HF.parse_size(chunk_size)
            (_,checksum,size) = XoloUtils.extract_path_sha256_size(path=path)
            op_chunks = Chunks.from_file(path=path, group_id=key, chunk_size=Some(_chunk_size))
            if op_chunks.is_none:
                raise EX.UnknownError(message=f"Failed to read the file: {path}")
            chunks = op_chunks.unwrap()

            num_chunks = len(chunks)
            semaphore = asyncio.Semaphore(max_concurrency)  # ✅ Limit concurrency to 10 uploads at a time
            # progress_bar = tqdm(total=len(value))
            async def upload_chunk(chunk:Chunk, attempt=1)->Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                while attempt <= max_tries:
                    try:
                        async with semaphore:  # ✅ Ensure controlled parallelism
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                client_id=self.client_id,
                                ball_id=key,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={"num_chunks": str(num_chunks), "full_checksum": checksum}
                            )
                        if res.is_ok:
                            return (None,Ok(res.unwrap()))  # ✅ Upload success
                    except Exception as e:
                        self.__log.error({
                            "event":"UPLOAD.CHUNK.FAILED",
                            "detail":str(e)
                        })
                        self.__log.warning(f"Chunk {chunk.chunk_id} failed on attempt {attempt}/{max_tries}. Retrying...")
                        await asyncio.sleep(2 ** attempt)  # ✅ Exponential backoff
                    attempt += 1

                return (chunk,Err(Exception(f"Failed to upload chunk {chunk.chunk_id} after {max_tries} retries.")))

            # ✅ Execute uploads in parallel with retries
            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)

            # ✅ Check if any uploads failed
            failures = [res for res in results if res[1].is_err]
            if len(failures)>0:
                raise EX.PutChunksError(message="")

            self.__log.info({
                "event": "PUT",
                "bucket_id": _bucket_id,
                "key": _key,
                "response_time": T.time() - t1
            })
            return Ok(True)
                
               

        except Exception as e:
            _e = MictlanXError.from_exception(e)
            self.__log.debug({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(e)
        

    async def put(self, bucket_id: str, key: str, value: bytes, chunk_size: str = "256kb", rf: int = 1, timeout: int = 120,max_tries:int = 5,max_concurrency:int =10,tags:Dict[str,str]={})->Result[bool,EX.MictlanXError]:
        """Uploads chunks and retries failed ones up to `MAX_TRIES`."""
        try:
            t1         = T.time()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _key       = Utils.sanitize_str(key)
            checksum   = XoloUtils.sha256(value=value)
            chunks_op  = Chunks.from_bytes(data=value, group_id=key, chunk_size=Some(HF.parse_size(chunk_size)), chunk_prefix=Some(key))
            router     = self.rlb.get_router()
            if not chunks_op.is_some:
                raise ValueError("No valid chunks to upload.")

            chunks = chunks_op.unwrap()
            num_chunks = len(chunks)
            semaphore = asyncio.Semaphore(max_concurrency)  # ✅ Limit concurrency to 10 uploads at a time
            progress_bar = tqdm(total=len(value))
            async def upload_chunk(chunk:Chunk, attempt=1)->Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                while attempt <= max_tries:
                    try:
                        async with semaphore:  # ✅ Ensure controlled parallelism
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                ball_id=key,
                                client_id=self.client_id,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={"num_chunks": str(num_chunks), "full_checksum": checksum,**tags}
                            )
                        if res.is_ok:
                            progress_bar.update(chunk.size)
                            return (None,Ok(res.unwrap()))  # ✅ Upload success
                    except Exception as e:
                        self.__log.error({
                            "event":"UPLOAD.CHUNK.FAILED",
                            "detail":str(e)
                        })
                        self.__log.warning(f"Chunk {chunk.chunk_id} failed on attempt {attempt}/{max_tries}. Retrying...")
                        await asyncio.sleep(2 ** attempt)  # ✅ Exponential backoff
                    attempt += 1

                return (chunk,Err(Exception(f"Failed to upload chunk {chunk.chunk_id} after {max_tries} retries.")))

            # ✅ Execute uploads in parallel with retries
            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)
            progress_bar.close()
            # ✅ Check if any uploads failed
            failures = [res for res in results if res[1].is_err]
            if len(failures)>0:
                raise EX.PutChunksError(message="")

            self.__log.info({
                "event": "PUT",
                "bucket_id": _bucket_id,
                "key": _key,
                "response_time": T.time() - t1
            })
            return Ok(True)

        except Exception as e:
            _e = MictlanXError.from_exception(e)
            self.__log.debug({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(e)

    
    async def get(self,
        bucket_id:str,
        key:str,
        max_paralell_gets:int = 10,
        headers:Dict[str,str]={},
        chunk_size:str="256kb", 
        timeout:int = 120,
        http2:bool = False
    )->Result[memoryview, EX.MictlanXError]:
        try:
            t1                    = T.time()
            _bucket_id            = Utils.sanitize_str(bucket_id)
            _key                  = Utils.sanitize_str(key)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = headers.get("Accept-Encoding","identity")
            router                = self.rlb.get_router()
            metadata_result = await router.get_metadata(_bucket_id, f"{_key}_0")
            if metadata_result.is_ok:
                metadata = metadata_result.unwrap()
                num_chunks = int(metadata.metadata.tags.get("num_chunks"))
                if num_chunks <= 0:
                    raise EX.ValidationError(message=f"No valid numuber of chunks: {num_chunks}")
                
                pbar = tqdm(total=num_chunks)
                async with httpx.AsyncClient(http2=http2,trust_env=False, timeout=timeout,verify=self.verify, headers=headers) as client:
                    semaphore = asyncio.Semaphore(max_paralell_gets)  # Limit to 10 parallel requests
                    async def fetch_chunk(i):
                        """Fetches chunk asynchronously with controlled concurrency."""
                        async with semaphore:
                            t2 = T.time()
                            res= await AsyncClientUtils.get_chunk(client=client,router=router,bucket_id=_bucket_id, key=f"{_key}_{i}",chunk_size=chunk_size, headers=headers)
                            elapsed = T.time()-t2
                            if res.is_ok:
                                pbar.set_postfix({'chunk': i, 'resp_time': f"{elapsed:.2f}s"})
                                pbar.update(n=1)
                            # self.__log.info({
                            #     "event":"GET.CHUNK",
                            #     "bucket_id":_bucket_id,
                            #     "key":_key,
                            #     "response_time":T.time()-t2
                            # })
                            return res
                    futures = [fetch_chunk(i) for i in range(num_chunks)]

                    results = await asyncio.gather(*futures)
                    # print("FUTURES", len(results))
                responses:List[Tuple[InterfaceX.Metadata, memoryview]] = list(map(lambda x:x.unwrap(),filter(lambda x:x.is_ok, results) ))
                # print(responses)
                if len(responses) ==0:
                    raise EX.NotFoundError("No chunks were found")
                elif len(responses) != num_chunks:
                    raise EX.NotFoundError(f"Some chunks were missing: expected = {num_chunks}, chunks={len(responses)}")
                
                pbar.close()
                remote_checksum = responses[0][0].tags.get("full_checksum","")
                x = await AsyncClientUtils.merge_chunks(chunks=responses)
                checksum = XoloUtils.sha256(x.tobytes())
                if not remote_checksum == checksum:
                    # self.__log.warning({
                    #     "event":"INTEGRITY.CHECK.FAILED",
                    #     "remote_checksum":remote_checksum,
                    #     "local_checksum":checksum
                    # })
                    raise EX.IntegrityError( message=f"Integrity check failed, remote not match with local checksum: {remote_checksum} != {checksum}")
                self.__log.info({ 
                    "event":"GET",
                    "bucket_id":_bucket_id,
                    "key":_key,
                    "checksum":checksum,
                    "response_time": T.time() -t1
                })
                return Ok(x)
            raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())
        except Exception as e:
            _e = MictlanXError.from_exception(e)
            self.__log.debug({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(EX.MictlanXError.from_exception(e))

    # async def delete_all()
    async def delete(self,
        ball_id:str,
        bucket_id:str,
        timeout: int = 120,
        headers: Dict[str, str] = {}
    ):
        try:
            pass
        except Exception as e:
            self.__log.error({
                "msg": str(e)
            })
            return Err(e)
    async def delete_by_key(self,
                     key: str,
                     bucket_id: str = "",
                     timeout: int = 120,
                     headers: Dict[str, str] = {}) -> Result[InterfaceX.DeletedByKeyResponse, Exception]:
        """
        Asynchronously deletes the data associated with the given key from the specified bucket.
        
        Args:
            key (str): The key for the data to be deleted.
            bucket_id (str): The ID of the bucket from which the data will be deleted. Defaults to an empty string.
            timeout (int): The timeout for the operation in seconds. Defaults to 120 seconds.
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.
        
        Returns:
            Result[InterfaceX.DeleteByKeyResponse, Exception]: The result of the delete operation.
        """
        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id == "" else _bucket_id
        try:
            failed = []
            del_res = InterfaceX.DeletedByKeyResponse(n_deletes=0, key=key)
            for router in self.__routers:
                start_time = T.time()
                # Await the async delete call on each router.
                del_result = await router.delete(bucket_id=_bucket_id, key=_key, headers=headers)
                if del_result.is_err:
                    response_time = T.time() - start_time
                    self.__log.error({
                        "event": "DELETE",
                        "bucket_id": _bucket_id,
                        "key": key,
                        "router_id": router.router_id,
                        "response_time": response_time
                    })
                    failed.append(router)
                else:
                    x_response = del_result.unwrap()
                    del_res.n_deletes += x_response.n_deletes
                    response_time = T.time() - start_time
                    self.__log.info({
                        "event": "DELETE",
                        "bucket_id": _bucket_id,
                        "key": _key,
                        "n_deletes": del_res.n_deletes,
                        "router_id": router.router_id,
                        "response_time": response_time
                    })
            return Ok(del_res)
        except httpx.HTTPError as e:
            return Err(e)
        except Exception as e:
            self.__log.error({
                "msg": str(e)
            })
            return Err(e)
        

        
    async def get_bucket_metadata(
            self,
            bucket_id: str,
            router: InterfaceX.Router = NONE,
            timeout: int = 120,
            headers: Dict[str, str] = {}
        ) -> Result[InterfaceX.GetRouterBucketMetadataResponse, Exception]:
            """
            Asynchronously fetches the metadata for the specified bucket from the given router.

            Args:
                bucket_id (str): The ID of the bucket.
                router (InterfaceX.Router): The router to use for the request.
                timeout (int): The timeout for the request (in seconds).
                headers (Dict[str, str]): Optional HTTP headers to include in the request.

            Returns:
                Result[InterfaceX.GetRouterBucketMetadataResponse, Exception]: On success, returns an Ok-wrapped response; otherwise, returns an Err with the exception.
            """
            try:
                start_time = T.time()
                # Await the async call from the router.
                x = await router.get_bucket_metadata(bucket_id=bucket_id, timeout=timeout, headers=headers)
                service_time = T.time() - start_time
                if x.is_ok:
                    self.__log.info({
                        "event": "GET.BUCKET.METADATA",
                        "bucket_id": bucket_id,
                        "router_id": router.router_id,
                        "service_time": service_time
                    })
                    return x
                else:
                    self.__log.error({
                        "msg": str(x.unwrap_err()),
                        "bucket_id": bucket_id,
                        "router_id": router.router_id,
                        "service_time": service_time
                    })
                    return x

            except httpx.HTTPError as e:
                self.__log_response_error(e)
                return Err(e)
            except Exception as e:
                self.__log.error({
                    "msg": str(e)
                })
                return Err(e)
    async def delete_bucket(self, bucket_id: str, headers: Dict[str, str] = {}, timeout: int = 120) -> Result[InterfaceX.DeleteBucketResponse, Exception]:
        """
        Asynchronously deletes the specified bucket by fetching metadata from each router,
        then concurrently deleting each object (ball) found.

        Args:
            bucket_id (str): The ID of the bucket to delete.
            headers (Dict[str, str]): Optional HTTP headers.
            timeout (int): Request timeout in seconds.

        Returns:
            Result[InterfaceX.DeleteBucketResponse, Exception]: An Ok-wrapped response on success,
            or an Err with an Exception on failure.
        """
        start_time = T.time()
        deleted = 0
        failed = 0
        total = 0
        keys = []  # Collect keys if needed

        deletion_tasks = []

        # For each router, schedule an asynchronous get_bucket_metadata call.
        metadata_tasks = [
            router.get_bucket_metadata(bucket_id=bucket_id, headers=headers, timeout=timeout)
            for router in self.__routers
        ]
        
        # Wait for all metadata calls to complete concurrently.
        metadata_results = await asyncio.gather(*metadata_tasks, return_exceptions=True)
        
        # Process each router's metadata result.
        for router, meta_result in zip(self.__routers, metadata_results):
            if meta_result.is_err:
                self.__log.error({
                    "msg": str(meta_result.unwrap_err()),
                    "bucket_id": bucket_id,
                    "router_id": router.router_id
                })
                continue
            if meta_result.is_ok:
                metadata = meta_result.unwrap()
                total += len(metadata.balls)
                # Schedule delete tasks for each ball (object) in the metadata.
                for ball in metadata.balls:
                    deletion_tasks.append(
                        self.delete_by_key(key=ball.key, bucket_id=bucket_id, headers=headers, timeout=timeout)
                    )
            else:
                self.__log.error({
                    "msg": str(meta_result.unwrap_err()),
                    "bucket_id": bucket_id,
                    "router_id": router.router_id
                })
        
        # Execute all deletion tasks concurrently.
        deletion_results = await asyncio.gather(*deletion_tasks, return_exceptions=True)
        
        for result in deletion_results:
            if isinstance(result, Exception):
                failed += 1
            else:
                if result.is_ok:
                    deleted += 1
                else:
                    failed += 1

        rt = T.time() - start_time
        self.__log.info({
            "event": "DELETED.BUCKET",
            "bucket_id": bucket_id,
            "deleted": deleted,
            "failed": failed,
            "total": total,
            "response_time": rt
        })
        
        return Ok(InterfaceX.DeleteBucketResponse(
            bucket_id=bucket_id,
            deleted=deleted,
            failed=failed,
            total=total,
            keys=keys,
            response_time=rt
        ))
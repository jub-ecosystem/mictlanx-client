import os
import time as T
import socket as S
import uuid 
import hashlib as H
import mictlanx.utils.time_unit as TU 
from threading import Lock
from typing import Tuple
# 
from mictlanx.logger.log import Log,DumbLogger
from mictlanx.v2.interfaces import responses as Responses
from mictlanx.v2.interfaces import requests  as Requests
import numpy as np
import numpy.typing as npt
from option import Result,Err,Ok
from mictlanx.v2.interfaces.metadata import Metadata
from mictlanx.v2.services.auth import AuthService
from mictlanx.v2.services.replica_manager import ReplicaManagerService
from mictlanx.v2.services.storage_node import StorageNodeService
from retry import retry

class Client(object):
    lock           = Lock()
    def __init__(self,**kwargs):
        self.client_id_size = kwargs.get("client_id_size",6)
        suffix = str(uuid.uuid4())[:self.client_id_size]
        self.client_id      = '{}-{}'.format('client',suffix )
        # 
        self.auth_hostname  = kwargs.get("auth_hostname","localhost")
        self.auth_port      = kwargs.get("auth_port",10000)
        # 
        self.rm_hostname    = kwargs.get("rm_hostname","localhost")
        self.rm_port        = kwargs.get("rm_port",20000)
        # 
        # 
        self.password       = kwargs.get("password","t0p#53cR3T")
        self.logger         = kwargs.get("log",DumbLogger())
        self.sink_path      = kwargs.get("sink_path")
        self.in_cache       = {}
        
        self.auth_service   = AuthService(service_id = "auth-{}".format(suffix),ip_addr = self.auth_hostname,port=self.auth_port, logger=self.logger)
        result              = self.auth_service.generate_token(password = self.password, client_id = self.client_id,headers= {})
        if(result.is_ok):
            generate_token_response = result.unwrap()
            self.token  = generate_token_response.token
            self.jti    = generate_token_response.json_web_token_id
        
        self.rm_service = ReplicaManagerService(service_id = "rm-{}".format(suffix),ip_addr = self.rm_hostname, port=self.rm_port, logger = self.logger)

    def __enter__(self):
        return self


    def shutdown(self):
        rm_exit_result = self.rm_service.exit()
        auth_exit_result = self.auth_service.exit()

    def __exit__(self,a,b,c):
        self.shutdown()
        return
    
    def put(self,**kwargs)-> Result[Responses.PutResponse,Responses.ErrorResponse]:
        with Client.lock:
            start_time        = T.time()
            key         = kwargs.get("key")
            value       = kwargs.get("value",bytearray())
            tags        = kwargs.get("tags",{})
            headers     = {
                "token":self.token,
                "sub":self.client_id,
                "jti":self.jti,
                "operation":"PUT",
                "key":key,
                "size":str(len(kwargs.get("value",[])))
            }
            path = "{}/{}".format(self.sink_path,key)
            
            if(key in self.in_cache and os.path.exists(path)):
                os.remove(path)
                del self.in_cache[key]

            def process_put(x:Responses.PutResponse)->Result[Responses.PutResponse,Responses.ErrorResponse]:
                response_time              = TU.sec_to_nanos(T.time() - start_time)
                x.response_time = response_time
                return Ok(x)
            def log_put_response(x:Responses.PutResponse):
                self.logger.info("PUT {} {} {} {} {}".format(x.response_id,x.metadata.id,x.metadata.size,x.service_time,x.response_time))
                return Ok(x)
            @retry(delay = 2, jitter= (0,5), tries=10)
            def __inner_balance()->Result[StorageNodeService,Responses.ErrorResponse]:
                try:
                    def process_balance(x:Responses.Balanced)->Result[StorageNodeService,Responses.ErrorResponse]:
                        self.logger.info("BALANCE {} {} {} {}".format(x.response_id, x.node_id, key, x.service_time))
                        self.logger.debug("STORAGE_NODE {} {} {}".format(x.node_id,x.ip_addr,x.port))
                        storage_node               = StorageNodeService(service_id = x.node_id,ip_addr = x.ip_addr, port = x.port,logger=self.logger)
                        return Ok(storage_node)

                    response = self.rm_service.balance(headers=headers).flatmap(process_balance)
                    # T.sleep(5)
                    return Responses.check_result_or_retry(response)
                except Exception as e:
                    self.logger.debug("WAITING_TIME RM {} {} {}".format(key,"PUT" ,T.time() - start_time))
                    self.logger.error("ERROR {}".format(e))
                    raise e
                    # return Err(e)
            @retry(delay = 2, jitter= (0,5), tries=10)
            def __inner_put(storage_node:StorageNodeService)->Result[Tuple[Responses.PutResponse,StorageNodeService],Responses.ErrorResponse]:
                try:
                    self.logger.debug("__INNER_PUT")
                    put_headers = {"checksum":kwargs.get("checksum",""),"client_id": self.client_id, "token": self.token,"to_disk":"1","tags":tags } 
                    sn_result   = Responses.check_result_or_retry(storage_node.put(key=key, value = value, headers=put_headers).flatmap(process_put).flatmap(log_put_response))
                    self.logger.debug("SN_RESULT"+str(sn_result))
                    if(sn_result.is_err):
                        return sn_result
                    else:
                        return Ok((sn_result.unwrap(),storage_node))
                except Exception as e:
                    self.logger.debug("WAITING_TIME SN {} {} {}".format(key,"PUT" ,T.time() - start_time))
                    self.logger.error("ERROR {}".format(e))
                    raise e
            def __inner_exit(x: Tuple[Responses.PutResponse, StorageNodeService]) -> Result[Responses.PutResponse,Responses.ErrorResponse]:
                x[1].exit()
                return Ok(x[0])

            
            return __inner_balance().flatmap(__inner_put).flatmap(__inner_exit)
    
    def put_ndarray(self,**kwargs) -> Result[Responses.PutResponse,Responses.ErrorResponse]:
        try:
            ndarray:npt.NDArray = kwargs.pop("ndarray",np.array([]))
            ttype = type(ndarray)
            if(ttype == np.ndarray ):
                _tags  = kwargs.pop("tags",{})
                tags   = {**_tags,"shape": str(ndarray.shape),"dtype":str(ndarray.dtype)}
                value  = ndarray.tobytes()
                digest = H.sha256()
                digest.update(value)
                checksum          = digest.digest().hex()
                return self.put(key = kwargs.pop("key"), value = value, tags = tags, checksum = checksum )
            else:
                return Err("ndarray must be a NDArray type.")
        except Exception as e:
            self.logger.error(str(e))
            return Err(e)


    def get(self,**kwargs)->Result[Responses.GetBytesResponse,Responses.ErrorResponse]:
        start_time        = T.time()
        key               = kwargs.get("key")
        cache             = kwargs.get("cache",False)
        force             = kwargs.get("force",True)
        get_headers       = {"client_id": self.client_id, "token": self.token} 


        def __get(**kwargs):
            try:
                key         = kwargs.get("key")
                headers     = {
                    "token":self.token,
                    "sub":self.client_id,
                    "jti":self.jti,
                    "operation":"GET",
                    "key":key,
                    "size":str(len(kwargs.get("value",[])))
                }
                # balance_result = self.rm_service.balance(headers=headers)

                @retry(delay = 2, jitter= (0,5), tries=10)
                def __inner_balance()->Result[StorageNodeService,Responses.ErrorResponse]:
                    try:
                        # with Client.lock:
                        def process_balance(x:Responses.Balanced)->Result[StorageNodeService,Responses.ErrorResponse]:
                            self.logger.info("BALANCE {} {} {} {}".format(x.response_id, x.node_id, key, x.service_time))
                            self.logger.debug("STORAGE_NODE {} {} {}".format(x.node_id,x.ip_addr,x.port))
                            storage_node               = StorageNodeService(service_id = x.node_id,ip_addr = x.ip_addr, port = x.port,logger=self.logger)
                            return Ok(storage_node)

                        response = self.rm_service.balance(headers=headers).flatmap(process_balance)
                        return Responses.check_result_or_retry(response)
                    except Exception as e:
                        self.logger.debug("WAITING_TIME RM {} {} {}".format(key,"PUT" ,T.time() - start_time))
                        self.logger.error("ERROR {}".format(e))
                        raise e
                @retry(delay = 2, jitter= (0,5), tries=10)
                def __inner_get(storage_node:StorageNodeService)->Result[Tuple[Responses.GetBytesResponse, StorageNodeService], Responses.ErrorResponse]:
                    # return Responses.check_result_or_retry(get_result)
                    try:
                        self.logger.debug("__INNER_GET")
                        get_result  = Responses.check_result_or_retry(storage_node.get(key=key,headers=get_headers, sink_path = self.sink_path, cache = cache))
                        self.logger.debug("SN_RESULT"+str(get_result))
                        if(cache):
                            self.in_cache[key] = get_response.metadata
                        if(get_result.is_err):
                            return get_result
                        else:
                            get_response = get_result.unwrap()
                            self.logger.info("GET {} {} {} {} {}".format(get_response.response_id, get_response.metadata.id,  get_response.metadata.size, get_response.service_time, get_response.response_time))
                            return Ok((get_response,storage_node))
                    except Exception as e:
                        self.logger.debug("WAITING_TIME SM {} {} {}".format(key,"GET" ,T.time() - start_time))
                        self.logger.error("ERROR {}".format(e))
                        raise e
                def __inner_exit(x: Tuple[Responses.PutResponse, StorageNodeService]) -> Result[Responses.PutResponse,Responses.ErrorResponse]:
                    x[1].exit()
                    return Ok(x[0])



                return __inner_balance().flatmap(__inner_get).flatmap(__inner_exit)
            except Exception as e:
                self.logger.error("GET_ERROR {}".format(e))
                return Err(e)
        with Client.lock:
            if(force):
                return __get(**kwargs)
            else:
                if(key in self.in_cache and cache):
                    # print("IN_CACHE")
                    metadata = self.in_cache[key]
                    # print("METADATA",metadata)
                    try:
                        with open("{}/{}".format(self.sink_path,key),"rb") as f:
                            # m        = H.sha256()
                            value    = f.read()
                            # m.update(value)
                            # checksum = m.hexdigest()
                            # size     = len(value)
                            response_time = TU.sec_to_nanos(T.time() - start_time)
                            get_response = Responses.GetNDArrayResponse(response_id="get-local",status=0, service_time=response_time,throughput=metadata.size/response_time, response_time=response_time, metadata = metadata.to_dict(), value=value)
                            return Ok(get_response)
                    except Exception as e:
                        return Err(e)
                else:
                    return __get(**kwargs)

    def get_ndarray(self,**kwargs)->Result[Responses.GetNDArrayResponse,Responses.ErrorResponse]:
        get_result = self.get(**kwargs)
        if(get_result.is_ok):
            get_response       = get_result.unwrap()
            value              = get_response.value
            shape              = eval(get_response.metadata.tags.get("shape"))
            dtype              = get_response.metadata.tags.get("dtype")
            print(shape,dtype)
            get_response.value = np.frombuffer(value,dtype = dtype).reshape(shape)
            return Ok(get_response)
        else:
            return get_result
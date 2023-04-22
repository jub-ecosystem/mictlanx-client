import requests as R 
import hashlib
from mictlanx.v3.services.auth import Auth
from mictlanx.v3.services.replica_manger import ReplicaManager
from mictlanx.v3.services.storage_node import StorageNode
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.interfaces.replica_manager import PutPayload as RMPutPayload,GetPayload as RMGetPayload, CompleteOperationPayload
from mictlanx.v3.interfaces.storage_node import PutPayload as SNPutPayload , PutResponse as SNPutResponse
from mictlanx.v3.interfaces.payloads import GenerateTokenPayload,PutPayload,GetPayload,PutNDArrayPayload
from mictlanx.v3.interfaces.responses import GenerateTokenResponse,GetBytesResponse,GetNDArrayResponse
from mictlanx.v3.interfaces.errors import ApiError 
import magic as M
import time as T
import numpy as np
import pandas as pd
import numpy.typing as npt
from option import Result,Ok,Err
from nanoid import generate as nanoid_
from lfu_cache import LFUCache
from mictlanx.logger.log import DumbLogger,Log
import logging as L

class Client(object):
    def __init__(self,**kwargs):
        self.rm_service:ReplicaManager = kwargs.get("rm_service")
        self.auth_service:Auth         = kwargs.get("auth_service")
        self.proxy:Proxy             = kwargs.get("proxy")
        self.proxies:list[Proxy]       = kwargs.get("proxy_pool")
        self.logger         = Log(
            name = "mictlanx-client-0",
            console_handler_filter = lambda record: record.levelno == L.INFO or record.levelno == L.ERROR,
        )
        # kwargs.get("log",DumbLogger())
        m = hashlib.sha256()
        self.password:str          = kwargs.get("password","root123")
        m.update(self.password.encode("utf8"))
        self.password = m.hexdigest()
        payload = GenerateTokenPayload(password = self.password)
        generate_token_response:GenerateTokenResponse = self.auth_service.generate_token(payload)
        self.token = generate_token_response.token
        self.client_id          = generate_token_response.client_id
        self.cache              = LFUCache(kwargs.get("limit",10))

    def put_ndarray(self,payload:PutNDArrayPayload,**kwargs)->Result[SNPutResponse,ApiError]:
        response = self.put(payload.into(),**kwargs)
        return response
    
    def put(self,payload:PutPayload,**kwargs)->Result[SNPutResponse,ApiError]:
        start_time = T.time()
        cache = kwargs.get("cache",False)
        payload.metadata = {**payload.metadata,"content_type":M.from_buffer(payload.bytes,mime=True),"checksum":payload.get_hash()}
        put_res  = self.proxy.put(payload,
                              headers={"Force":str(kwargs.get("force",1)),"Client-Id":self.client_id,"Authorization":self.token,"password":self.password}
        )
        if(cache):
            self.cache.put(payload.key,(payload.metadata,payload.bytes))
        
        response_time = T.time() - start_time
        self.logger.info("PUT {} {} {}".format(payload.key,len(payload.bytes),response_time))
        return put_res
            

    def get(self,**kwargs)->Result[GetBytesResponse,ApiError]:
        start_time = T.time()
        cache  = kwargs.get("cache",False)
        force  = kwargs.get("force",True)
        key    = kwargs.get("key")
        result:Result[GetBytesResponse,ApiError] = Err(None)
        if(force):
            result = self.proxy.get(
                    key, 
                    {"Client-Id":self.client_id,"Authorization":self.token,"password":self.password}
            )
            # return result
        else:
            if(cache  and key in self.cache.cache ):
                element = self.cache.get(key)
                metadata,value = element
                result = Ok(GetBytesResponse(value =value,metadata =metadata,response_time=0 ))
                # return result
            else:
                result = self.proxy.get(
                        key, 
                        {"Client-Id":self.client_id,"Authorization":self.token,"password":self.password}
                )

        response_time = T.time() - start_time
        if(result.is_ok):
            response = result.unwrap()
            self.logger.info("GET {} {} {}".format(key,len(response.value),response_time))
            return result
        else:
            error = result.unwrap_err()
            self.logger.error(str(error))
        return result
    def get_ndarray(self,**kwargs)->Result[GetNDArrayResponse,ApiError]:
        start_time = T.time()
        result = self.get(**kwargs)
        if(result.is_ok):
            response = result.unwrap()
            metadata = response.metadata
            dtype    = metadata.get("dtype",np.float64)
            ndarray  = np.frombuffer(response.value,dtype=dtype)
            shape    = eval(metadata.get("shape",str(ndarray.shape)))
            ndarray  = ndarray.reshape(shape)
            response_time = T.time() - start_time
            self.logger.info("GET_NDARRAY {} {} {}".format(kwargs.get("key"),len(response.value),response_time))
            return Ok(GetNDArrayResponse(value = ndarray, metadata = metadata, response_time = response_time))
        else:
            error = result.unwrap_err()
            self.logger.error(str(error))
            return result
import requests as R 
import hashlib
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.replica_manger import ReplicaManager
from mictlanx.v3.services.proxy import Proxy
from mictlanx.v3.interfaces.storage_node import  PutResponse as SNPutResponse
from mictlanx.v3.interfaces.payloads import AuthTokenPayload,PutPayload,PutNDArrayPayload,SignUpPayload
from mictlanx.v3.interfaces.responses import AuthResponse,GetBytesResponse,GetNDArrayResponse
from mictlanx.v3.interfaces.errors import ApiError 
import magic as M
import time as T
import numpy as np
from option import Result,Ok,Err
from lfu_cache import LFUCache
from mictlanx.logger.log import Log
import logging as L
from durations import Duration
from option import Option,NONE,Some
from nanoid import generate as nanoid_
from typing import Dict



class Client(object):
    def __init__(self,app_id:str = None,client_id:Option[str]=NONE,metadata:Option[Dict[str,str]]=NONE,replica_manager:ReplicaManager=None, xolo:Xolo = None, proxy:Proxy = [],secret:str = None,expires_in:Option[str] = NONE,cache_limit:int=10):
        self.app_id                    = app_id
        self.client_id                 = client_id.unwrap_or(nanoid_())
        self.rm_service:ReplicaManager = replica_manager
        self.xolo:Xolo                 = xolo
        self.proxy:Proxy               = proxy
        self.proxies:list[Proxy]       = [self.proxy]
        self.logger         = Log(
            name = "mictlanx-client",
            console_handler_filter = lambda record: record.levelno == L.INFO or record.levelno == L.ERROR,
        )
        # _____________________________________________________________
        # m = hashlib.sha256()
        self.secret:str          = secret
        # m.update(self.secret.encode("utf8"))
        # self.secret = m.hexdigest()
        # _expires_in = expires_in.unwrap_or("1d")
        
        payload = AuthTokenPayload(app_id=app_id,client_id=self.client_id,secret = self.secret,expires_in  =expires_in )
        signup_payload = SignUpPayload(app_id=self.app_id,client_id=self.client_id,secret=self.secret,metadata=metadata.unwrap_or({}),expires_in=expires_in)
        auth_result = self.xolo.authenticate_or_signup(payload,Some(signup_payload))
        if(auth_result.is_ok):
            auth_response = auth_result.unwrap()
            self.token = auth_response.token
        else:
            error = auth_result.unwrap_err()
            raise error
        self.cache              = LFUCache(cache_limit)

    def put_ndarray(self,payload:PutNDArrayPayload,cache:bool = False, update:bool = False)->Result[SNPutResponse,ApiError]:
        response = self.put(payload=payload.into(),cache=cache,update=update)
        return response
    
    def put(self,payload:PutPayload=None,cache:bool = False, update:bool = False)->Result[SNPutResponse,ApiError]:
        start_time = T.time()
        if(update):
            self.proxy.delete(ball_id=payload.key,headers={})
        payload.metadata = {"content_type":M.from_buffer(payload.bytes,mime=True),**payload.metadata,"checksum":payload.get_hash(),'client_id':self.client_id}
        put_res  = self.proxy.put(
            payload,
            headers={
                "Application-Id":self.app_id,
                "Client-Id":self.client_id,
                "Authorization":self.token,
                "Secret":self.secret
            }
        )
        if(cache):
            self.cache.put(payload.key,(payload.metadata,payload.bytes))
        
        response_time = T.time() - start_time
        self.logger.info("PUT {} {} {}".format(payload.key,len(payload.bytes),response_time))
        return put_res
            

    def delete(self,key:str = None)->Result[str,ApiError]:
        return self.proxy.delete(ball_id=key,headers={})
    
    def get(self,key:str=None, cache:bool=False, force:bool=True)->Result[GetBytesResponse,ApiError]:
        start_time = T.time()
        # cache  = kwargs.get("cache",False)
        # force  = kwargs.get("force",True)
        # key    = kwargs.get("key")
        result:Result[GetBytesResponse,ApiError] = Err(None)
        if(force):
            result = self.proxy.get(
                    key, 
                    {"Client-Id":self.client_id,"Authorization":self.token,"password":self.secret}
            )
            # return result
        else:
            if(cache and key in self.cache.cache ):
                element = self.cache.get(key)
                metadata,value = element
                self.logger.info("HIT {} {} 0".format(key, len(value)))
                result = Ok(GetBytesResponse(value =value,metadata =metadata,response_time=0 ))
                # return result
            else:
                self.logger.info("MISS {} {} 0".format(key, 0 ))
                result = self.proxy.get(
                        key, 
                        {"Client-Id":self.client_id,"Authorization":self.token,"password":self.secret}
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
    def get_ndarray(self,key:str=None, cache:bool=False, force:bool=True)->Result[GetNDArrayResponse,ApiError]:
        start_time = T.time()
        result = self.get(key=key,cache=cache,force=force)
        if(result.is_ok):
            response = result.unwrap()
            metadata = response.metadata
            dtype    = metadata.get("dtype",np.float64)
            ndarray  = np.frombuffer(response.value,dtype=dtype)
            shape    = eval(metadata.get("shape",str(ndarray.shape)))
            ndarray  = ndarray.reshape(shape)
            response_time = T.time() - start_time
            self.logger.info("GET_NDARRAY {} {} {}".format(key,len(response.value),response_time))
            return Ok(GetNDArrayResponse(value = ndarray, metadata = metadata, response_time = response_time))
        else:
            error = result.unwrap_err()
            self.logger.error(str(error))
            return result
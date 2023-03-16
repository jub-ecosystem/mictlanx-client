import time as T
import socket as S
import uuid 
import hashlib as H
import mictlanx.utils.time_unit as TU 
from threading import Lock
import logging
# 
from mictlanx.logger.log import Log
from mictlanx.v2.interfaces import responses as Responses
# import GetResponse,PutResponse,Exited,GeneratedToken,Balanced,ClientCodec,ErrorResponse
from mictlanx.v2.interfaces import requests  as Requests
# import Get,Put,Exit,GenerateToken,Balance
import numpy as np
import numpy.typing as npt
# from mictlanx.interfaces.statues import Status
# import json
from option import Result,Err,Ok
from mictlanx.v2.services.auth import AuthService
from mictlanx.v2.services.replica_manager import ReplicaManagerService
from mictlanx.v2.services.storage_node import StorageNodeService

class Client(object):
    lock           = Lock()
    def __init__(self,**kwargs):
        self.client_id_size = kwargs.get("client_id_size",6)
        self.client_id      = '{}-{}'.format('client',str(uuid.uuid4())[:self.client_id_size])
        self.auth_hostname  = kwargs.get("auth_hostname","localhost")
        self.auth_port      = kwargs.get("auth_port",10000)
        self.rm_hostname    = kwargs.get("rm_hostname","localhost")
        self.rm_port        = kwargs.get("rm_port",20000)
        self.auth_service   = AuthService(ip_addr = self.auth_hostname,port=self.auth_port)
        self.rm_service     = ReplicaManagerService(ip_addr = self.rm_hostname, port=self.rm_port)
        self.password       = kwargs.get("password","t0p#53cR3T")
        self.log_kwargs     = kwargs.get("log_kwargs",{})
        self.disabled_log   = kwargs.get("disabled_log",False)
        self.logger         = Log(name = self.client_id, level = logging.DEBUG, disabled = self.disabled_log, **self.log_kwargs)
        result              = self.auth_service.generate_token(password = self.password, client_id = self.client_id,headers= {})
        # print("GENERATE_TOKEN_RESULT",result)
        if(result.is_ok):
            generate_token_response = result.unwrap()
            # print("GENERATE_RESPONSE",generate_token_response)
            self.token = generate_token_response.token
            self.jti   = generate_token_response.json_web_token_id

    def __enter__(self):
        return self


    def shutdown(self):
        rm_exit_result = self.rm_service.exit()
        auth_exit_result = self.auth_service.exit()




    def __exit__(self,a,b,c):
        self.shutdown()
        return

    # def __map_response_time(self,x:Responses.PutResponse):
        # return 
    def put(self,**kwargs)-> Result[Responses.PutResponse,Responses.ErrorResponse]:
        start_time        = T.time()
        try:
            with Client.lock:
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
                balance_result = self.rm_service.balance(headers=headers)
                if(balance_result.is_ok):
                    balance_response           = balance_result.unwrap()
                    storage_node               = StorageNodeService(ip_addr = balance_response.ip_addr, port = balance_response.port)
                    put_headers                = {"checksum":kwargs.get("checksum",""),"client_id": self.client_id, "token": self.token,"to_disk":"1","tags":tags } 
                    put_result                 = storage_node.put(key=key, value = value, headers=put_headers)
                    response_time              = TU.sec_to_nanos(T.time() - start_time)
                    put_response               = put_result.unwrap()
                    put_response.response_time = response_time
                    storage_node.exit()
                    return Ok(put_response)
                else:
                    return balance_result
        except Exception as e:
            self.logger.error("ERROR {}".format(e))
            return Err(e)
    def get(self,**kwargs)->Result[Responses.GetBytesResponse,Responses.ErrorResponse]:
        start_time        = T.time()
        try:
            with Client.lock:
                key         = kwargs.get("key")
                headers     = {
                    "token":self.token,
                    "sub":self.client_id,
                    "jti":self.jti,
                    "operation":"GET",
                    "key":key,
                    "size":str(len(kwargs.get("value",[])))
                }
                balance_result = self.rm_service.balance(headers=headers)
                if(balance_result.is_ok):
                    balance_response           = balance_result.unwrap()
                    print("HERE_1")
                    storage_node               = StorageNodeService(ip_addr = balance_response.ip_addr, port = balance_response.port)
                    get_headers                = {"checksum":kwargs.get("checksum",""),"client_id": self.client_id, "token": self.token} 
                    print("HERE_2")
                    get_result                 = storage_node.get(key=key,headers=get_headers)
                    print("GET_RESULT",get_result)
                    response_time              = TU.sec_to_nanos(T.time() - start_time)
                    get_response               = get_result.unwrap()
                    get_response.response_time = response_time
                    storage_node.exit(headers={"origin":"FROM_CLIENT"})
                    return Ok(get_response)
                else:
                    return balance_result
        except Exception as e:
            return Err(e)
    def get_ndarray(self,**kwargs)->Result[Responses.GetNDArrayResponse,Responses.ErrorResponse]:
        get_result = self.get(**kwargs)
        if(get_result.is_ok):
            get_response       = get_result.unwrap()
            print("GET_RESPONSE", get_response)
            value              = get_response.value
            # .decode("unicode-escape").encode("ISO-8859-1")
            shape              = eval(get_response.metadata.tags.get("shape"))
            dtype              = get_response.metadata.tags.get("dtype")
            print("VALUE",value)
            print("SHAPE",shape)
            print("DTYPE",dtype)
            get_response.value = np.frombuffer(value,dtype = dtype).reshape(shape)
            return Ok(get_response)
        else:
            return get_result

    def put_ndarray(self,**kwargs) -> Result[Responses.PutResponse,Responses.ErrorResponse]:
        try:
            ndarray:npt.NDArray = kwargs.pop("ndarray",np.array([]))
            ttype = type(ndarray)
            if(ttype == np.ndarray ):
                _tags = kwargs.pop("tags",{})
                tags  = {**_tags,"shape": str(ndarray.shape),"dtype":str(ndarray.dtype)}
                value = ndarray.tobytes()
                digest           = H.sha256()
                digest.update(value)
                checksum          = digest.digest().hex()
                # print("BEFORE PUT", checksum)
                return self.put(key = kwargs.pop("key"), value = value, tags = tags, checksum = checksum )
            else:
                return Err("ndarray must be a NDArray type.")
        except Exception as e:
            self.logger.error(str(e))
            return Err(e)
            # balance_req = Balance(headers = headers)

#             # put_request.update_headers(checksum = checksum)

#             # hash_service_time = TU.sec_to_nanos(T.time() - start_time)
#             # self.logger.debug("HASH_TIME {} {}".format(put_request.key,hash_service_time))
#             # print(put_request.headers)
#             # try:
#             #     self.socket.sendall(put_request.encode())
#             #     response = PutResponse.decode(socket = self.socket)
#             #     print("_"*20)
#             #     return response
#             # except Exception as e: 
#             #     self.logger.error("{}".format(e))
#             #     raise e
#     def put_ndarray(self,**kwargs):
#         nd_array:npt.NDArray = kwargs.get("ndarray",np.array([]))
#             headers = kwargs.pop("headers",{})
#             tags = {
#                     "shape":str(nd_array.shape),
#                     "dtype":str(nd_array.dtype),
#                     **headers.get("tags",{})
#             }
#             new_headers = {**headers,"tags":tags}
#             # put_response  = self.put(**{**kwargs},"value":nd_array.tobytes())
#             put_response  = self.put(**{**kwargs,"headers":new_headers})
#             return put_response
#         else:
#             return PutResponse.BadParameters()
#         # response_type = type(put_response)
#         # if(response_type == PutResponse.Success):

#         # else:
#         #     return put_response

                
#     def get(self,**kwargs):
#         with Client.lock:
#             start_time  = T.time()
#             get_request = Get(**kwargs)
#             socket      = self.socket
#             try:
#                 socket.sendall(get_request.encode())
#                 response               = GetResponse.decode(socket = socket)
#                 response.response_time = TU.sec_to_nanos(T.time() - start_time)
#                 print("_"*20)
#                 return response
#             except Exception as e: 
#                 self.logger.error("{}".format(e))
#                 raise e
#     def get_ndarray(self,**kwargs):
#         get_response = self.get(**kwargs)
#         response_type  = type(get_response)
#         if( response_type == GetResponse.Sucess):
#             tags               = get_response.metadata.tags
#             if not ("shape" in tags) or not ("dtype" in tags):
#                 raise Exception("Not shape or dtype found in tags")
#             shape = eval(tags.get("shape"))
#             dtype = tags.get("dtype")
#             get_response.value = np.frombuffer(get_response.value,dtype=dtype).reshape(shape)
#             return get_response
#         else:
#             return get_response
#     def exit(self,**kwargs):
#         with Client.lock:
#             start_time   = T.time()
#             socket = kwargs.pop("socket",self.socket)
#             exit_request = Exit(**kwargs)
#             # socket = self.socket
#             try:
#                 socket.sendall(exit_request.encode())
#                 response               = Exited.decode(socket = socket)
#                 response.response_time = TU.sec_to_nanos(T.time() - start_time)
#                 print("_"*20)
#                 return response
#             except Exception as e: 
#                 self.logger.error("{}".format(e))
#                 raise e
            
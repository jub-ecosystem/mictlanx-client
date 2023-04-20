import requests as R 
from retry import retry
import os
import hashlib
from mictlanx.v3.services.auth import Auth
from mictlanx.v3.services.replica_manger import ReplicaManager
from mictlanx.v3.services.storage_node import StorageNode
from mictlanx.v3.interfaces.replica_manager import PutPayload as RMPutPayload,GetPayload as RMGetPayload, CompleteOperationPayload
from mictlanx.v3.interfaces.storage_node import PutPayload as SNPutPayload , PutResponse as SNPutResponse
from mictlanx.v3.interfaces.payloads import GenerateTokenPayload,PutPayload,GetPayload,GetBytesResponse,GetNDArrayResponse,PutNDArrayPayload
from mictlanx.v3.interfaces.responses import GenerateTokenResponse
from mictlanx.v3.interfaces.errors import ApiError 
import time as T
import numpy as np
import pandas as pd
import numpy.typing as npt
from option import Result,Ok,Err
from nanoid import generate as nanoid_

class Client(object):
    def __init__(self,**kwargs):
        self.rm_service:ReplicaManager = kwargs.get("rm_service")
        self.auth_service:Auth         = kwargs.get("auth_service")
        m = hashlib.sha256()
        self.password:str          = kwargs.get("password","root123")
        m.update(self.password.encode("utf8"))
        self.password = m.hexdigest()
        payload = GenerateTokenPayload(password = self.password)
        generate_token_response:GenerateTokenResponse = self.auth_service.generate_token(payload)
        self.authoriztion_token = generate_token_response.token
        self.client_id          = generate_token_response.client_id
        self.cache = {}
    
    def put_ndarray(self,payload:PutNDArrayPayload)->Result[SNPutResponse,ApiError]:
        response = self.put(payload.into())
        return response
        # try:
            # pass
        # except R.RequestException as e:
            # return e
    def put(self,payload:PutPayload,**kwargs)->Result[SNPutResponse,ApiError]:
        rm_payload = RMPutPayload(
            ball_id = payload.key, 
            size = len(payload.bytes),
            metadata = {**payload.metadata,"checksum":payload.get_hash()}
        )
        rm_put_result = self.rm_service.put(rm_payload,
            client_id = self.client_id,
            authorization = self.authoriztion_token, 
            password =self.password
        )
        if(rm_put_result.is_ok):
            rm_put_response = rm_put_result.unwrap()
            print("RM_PUT_RESPONSE",rm_put_response)
            sn_service = StorageNode(ip_addr = rm_put_response.ip_addr, port = rm_put_response.port , api_version =self.rm_service.api_version)
            sn_put_result = sn_service.put(key =payload.key ,bytes = payload.bytes)
            if(sn_put_result.is_ok):
                sn_put_response =sn_put_result.unwrap()
                rm_complete_operation_payload = CompleteOperationPayload(node_id = rm_put_response.node_id,operation_type="put",operation_id = rm_put_response.operation_id)
                complete_operation_result = self.rm_service.complete_operation(rm_complete_operation_payload)
                # print("COMPLETE_OPERATION_RESULT",complete_operation_result)
                if(complete_operation_result.is_ok):
                    return sn_put_result
            else:
                return sn_put_result
        else:
            return rm_put_result
            # raise 
        # return rm_put_result
            

    def get(self,**kwargs)->Result[GetBytesResponse,ApiError]:
        start_time     = T.time()
        key            = kwargs.get("key")
        cache          = kwargs.get("cache")
        rm_get_payload = RMGetPayload(key =  key)
        rm_get_result  = self.rm_service.get(rm_get_payload,client_id = self.client_id,authorization = self.authoriztion_token, password =self.password)
        if(rm_get_result.is_ok):
            rm_get_response = rm_get_result.unwrap()
            sn_service      = StorageNode(ip_addr = rm_get_response.ip_addr, port = rm_get_response.port , api_version =1)
            sn_get_result = sn_service.get(key=key)
            if(sn_get_result.is_ok):
                sn_get_response =  sn_get_result.unwrap()
                return Ok(
                    GetBytesResponse(
                        value = sn_get_response, 
                        metadata = rm_get_response.metadata,
                        response_time = T.time() - start_time
                    )
                )
            else:
                return sn_get_result.unwrap_err()
        else:
            return rm_get_result.unwrap_err()
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
            return Ok(GetNDArrayResponse(value = ndarray, metadata = metadata, response_time = response_time))
        return result
            # response.bytes

            # response

            # print("SN_GET_RESPONSE",sn_get_response)

        # print("RM_GET_RESULT",rm_get_result)



if __name__ == "__main__":
    rm_service   = ReplicaManager(ip_addr = "localhost", port=20001, api_version=1)
    auth_service = Auth(ip_addr = "localhost", port=10000, api_version=1)
    c            = Client(rm_service = rm_service, auth_service = auth_service,password = os.environ.get("PASSWORD"))
    ndarray = pd.read_csv("/source/source/batch1/balance_scale.csv").values
    put_payload = PutNDArrayPayload(key=nanoid_(),ndarray = ndarray)
    # x = c.put_ndarray(put_payload)
    # print(x)
    get_res      = c.get_ndarray(key ="-Deg-Hx-0llqT2WhzeIqb").unwrap()
    print(get_res)
    # with open("","rb") as f:
        # data = f.read()
        # put_payload      = PutPayload(key = nanoid_(), bytes =data, metadata={"filename":"balance_scale","extension":"csv"})
        # c.put(put_payload)
        # get_res      = c.get(key ="IcrfO1PWGWalrBbGuMUIp").unwrap()
    # print(get_res.value)
    # print("PUT_RES",put_res)
    
    
    
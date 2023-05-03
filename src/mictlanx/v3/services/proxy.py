import requests as R
from mictlanx.v3.interfaces.service import Service
from mictlanx.v3.interfaces.payloads import PutPayload,GetPayload
from mictlanx.v3.interfaces.responses import GetResponse,GetBytesResponse
from mictlanx.v3.interfaces.storage_node import PutPayload,PutResponse
from mictlanx.v3.interfaces.errors import ApiError,Unauthorized,ServerInternalError
from option import Result,Ok,Err
import time as T
import json as J 


class Proxy(Service):
    def __init__(self,*args,**kwargs):
        super(Proxy,self).__init__(*args,**kwargs)
        self.put_url = '{}'.format(self.base_url)
        self.get_url = lambda x: '{}/{}'.format(self.base_url,x)
        self.del_url = lambda x: '{}/{}'.format(self.base_url,x)
        self.get_metadata_url = lambda x: '{}/metadata/{}'.format(self.base_url,x)
    
    def delete(self,ball_id:str,headers:dict = {}):
        try:
            url = self.del_url(ball_id)
            response = R.delete(
                url,
                headers=headers
            )
            response.raise_for_status()
            # get_response = GetBytesResponse(value = response.content,metadata =metadata,response_time = T.time()- start_time)
            return Ok((ball_id))
        except Exception as e:
            if(type(e) is R.RequestException):
                response:R.Response = e.response
                return Err(ServerInternalError(message = response.headers.get("Error-Message"), metadata = response.headers  ))
            else:
                return Err(e)


    def get(self,ball_id:str,headers:dict):
        try:
            start_time = T.time()
            url = self.get_url(ball_id)
            response = R.get(
                url,
                headers=headers
            )
            metadata_response = R.get(
                self.get_metadata_url(ball_id),
                headers=headers
            )
            response.raise_for_status()
            metadata_response.raise_for_status()
            metadata = metadata_response.json()
            get_response = GetBytesResponse(value = response.content,metadata =metadata,response_time = T.time()- start_time)
            return Ok(get_response)
        except Exception as e:
            print(e)
            if(type(e) is R.RequestException):
                response:R.Response = e.response
                return Err(ServerInternalError(message = response.headers.get("Error-Message"), metadata = response.headers  ))
            else:
                return Err(e)
            # return Err(e)
    def put(self,payload:PutPayload,headers:dict):
        try:
            # print("URL",self.put_url)
            rm_put_payload = {
                "ball_id":payload.key,
                "ball_size":len(payload.bytes),
                "metadata": payload.metadata,
            }
            metadata_json=J.dumps(rm_put_payload)
            # print("METADATA_JSON",metadata_json)
            fields = {
                "metadata":(None, metadata_json,"application/json"),
                payload.key:(payload.key,payload.bytes, "application/octet-stream")
            }
            response = R.post(
                self.put_url,
                files = fields,
                headers=headers
            )
            response.raise_for_status()
            response_json = PutResponse(**response.json())
            return Ok(response_json)
        except Exception as e:
            print(e)
            if(type(e) is R.RequestException):
                response:R.Response = e.response
                return Err(ServerInternalError(message = response.headers.get("Error-Message"), metadata = response.headers  ))
            else:
                return Err(e)
            # response:R.Response = e.response
            # print(response)
            # return Err(ServerInternalError(message = response.headers.get("Error-Message"), metadata = response.headers  ))
    # def generate_token(self,payload:GenerateTokenPayload):
    #     try:
    #     #     print("URL",self.generate_token_url)
    #         response_data = payload.to_dict()
    #         # print("DATA 1",type(response_data))
    #         response = R.post(self.generate_token_url,json=response_data)
    #         response_data = GenerateTokenResponse(**response.json())
    #         # print(response_data)
    #         return response_data
    #         # print(data)
    #     except Exception as e:
    #         print("ERROR {}",e)
    #         raise e
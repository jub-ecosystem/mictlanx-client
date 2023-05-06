from abc import ABC
from nanoid import generate as nanoid_
import numpy as np
import numpy.typing as npt
import hashlib as H
from typing import TypeVar,Generic,Dict
from option import Option,Some,NONE
from durations import Duration

class Payload(ABC):
    def __init__(self,*args,**kwargs):
        pass
    def to_dict(self):
        return self.__dict__
class LogoutPayload(Payload):
    def __init__(self,app_id:str=None, client_id:str=None, token:str=None, secret:str=None):
        super(LogoutPayload,self).__init__()
        self.app_id:str    = app_id
        self.client_id:str = client_id
        self.token:str     = token
        self.secret:str    =  secret


class VerifyTokenPayload(Payload):
    def __init__(self,app_id:str=None,client_id:str=None,token:str=None,secret:str=None):
        super(VerifyTokenPayload,self).__init__()
        self.app_id:str    = app_id
        self.client_id:str = client_id
        self.token:str     = token
        self.secret:str    = secret

class RefreshTokenPayload(Payload):
    def __init__(self,app_id:str=None, client_id:str=None, token:str=None, secret:str=None):
        super(RefreshTokenPayload,self).__init__()
        self.app_id:str    = app_id
        self.client_id:str = client_id
        self.token:str     =  token
        self.secret:str    = secret

class SignUpPayload(Payload):
    def __init__(self,app_id:str=None, client_id:str=None, secret:str=None, metadata:Dict[str,str]={},claims:Dict[str,str]={},expires_in:Option[str]=NONE):
        super(SignUpPayload,self).__init__()
        self.app_id:str             = app_id
        self.client_id:str          = client_id
        self.secret:str             = secret
        self.metadata:Dict[str,str] = metadata
        self.claims:Dict[str,str]   = claims
        self.expires_in = int(Duration(expires_in.unwrap_or("1d")).to_seconds())

class AuthTokenPayload(Payload):
    def __init__(self,app_id:str = None, client_id:str=None,secret:str=None, expires_in:Option[str]=NONE):
        super(AuthTokenPayload,self).__init__()
        self.app_id     = app_id
        self.client_id  = client_id
        self.secret     = secret
        self.expires_in = int(Duration(expires_in.unwrap_or("1d")).to_seconds())

class GetPayload(Payload):
    def __init__(self,key:str=None):
        super(GetPayload,self).__init__()
        self.key:str = key

class PutPayload(Payload):
    def __init__(self,key:str=nanoid_(),data:bytes=None, metadata:Dict[str,str]=None):
        super(PutPayload,self).__init__()
        self.key:str                = key
        self.bytes:bytes            = data
        self.metadata:Dict[str,str] = metadata
    
    def __str__(self):
        return "PutPayload(key={})".format(self.key)
    
    def get_hash(self)->str:
        sha256 = H.sha256()
        sha256.update(self.bytes)
        return sha256.hexdigest()

class PutNDArrayPayload(Payload):
    def __init__(self,key:str=nanoid_(),ndarray:npt.NDArray=None,metadata:Dict[str,str]={}):
        super(PutNDArrayPayload,self).__init__()
        self.key   = key
        self.ndarray:npt.NDArray = ndarray
        self.metadata = metadata
        # kwargs.get("metadata",{})
    def into(self)->PutPayload:
        return PutPayload(
            data = self.ndarray.tobytes(), 
            key = self.key, 
            metadata = {**self.metadata, "shape": str(self.ndarray.shape),"dtype":str(self.ndarray.dtype)
                        }

        )
    


from typing import Generic,TypeVar
import numpy.typing as npt
T = TypeVar("T")

class SummonContainerResponse(object):
    def __init__(self,container_id:str,ip_addr:str, port:int, service_time:int):
        self.container_id = container_id
        self.service_time = service_time
        self.ip_addr= ip_addr
        self.port =port
        
        

class SignUpResponse(object):
    def __init__(self,token:str=None,service_time:int=None):
        self.token:str = token
        # kwargs.get("token")
        self.service_time = service_time
        # kwargs.get("service_time",0)
    def __str__(self):
        return "SignUpResponse(token={}, service_time={})".format(self.token,self.service_time)
class RefreshTokenResponse(object):
    def __init__(self,**kwargs):
        self.token = kwargs.get("token")
        self.service_time = kwargs.get("service_time",0)

class LogoutResponse(object):
    def __init__(self,**kwargs):
        self.logout = kwargs.get("logout",False)
        self.service_time = kwargs.get("service_time",0)

class VerifyTokenResponse(object):
    def __init__(self,**kwargs):
        self.verified = kwargs.get("verified",False)
        self.service_time = kwargs.get("service_time",0)


class AuthResponse(object):
    def __init__(self,client_id=None, token=None, jti=None, metadata={},service_time=None):
        self.client_id    = client_id
        self.token        = token
        self.jti          = jti
        self.metadata     = metadata
        self.service_time = service_time
    def __str__(self):
        return "AuthResponse(client_id={})".format(self.client_id)


class GetInMemoryResponse(object):
    def __init__(self,**kwargs):
        self.node_id       = kwargs.get("key")
        self.response_time = kwargs.get("response_time")
        self.bytes         = kwargs.get("bytes")

class GetResponse(Generic[T]):

    def __init__(self,*args,**kwargs):
        self.value:T   = kwargs.get("value",bytearray())
        self.metadata      = kwargs.get("metadata",{})
        self.response_time = kwargs.get("response_time")
class GetBytesResponse(GetResponse[bytes]):
    def __init__(self,*args,**kwargs):
        super(GetBytesResponse,self).__init__(*args,**kwargs)
    def __str__(self):
        return "GetResponse(response_time={}, size={})".format(self.response_time,len(self.value))
class GetNDArrayResponse(GetResponse[npt.NDArray]):
    def __init__(self,*args,**kwargs):
        super(GetNDArrayResponse,self).__init__(*args,**kwargs)
    def __str__(self):
        return "GetResponse(response_time={}, shape={})".format(self.response_time,self.value.shape)

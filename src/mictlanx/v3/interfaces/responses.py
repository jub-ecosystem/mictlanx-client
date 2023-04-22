
from typing import Generic,TypeVar
import numpy.typing as npt
T = TypeVar("T")

class GenerateTokenResponse(object):
    def __init__(self,**kwargs):
        self.client_id    = kwargs.get("client_id")
        self.token        = kwargs.get("token")
        self.jti          = kwargs.get("jti")
        self.service_time = kwargs.get("service_time")
    def __str__(self):
        return "GenerateTokenResponse(client_id={})".format(self.client_id)


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

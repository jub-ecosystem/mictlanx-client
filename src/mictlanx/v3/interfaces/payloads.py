from abc import ABC
from nanoid import generate as nanoid_
import numpy as np
import numpy.typing as npt
import hashlib as H
from typing import TypeVar,Generic

T = TypeVar("T")
class Payload(ABC):
    def __init__(self,*args,**kwargs):
        pass
    def to_dict(self):
        return self.__dict__

class GenerateTokenPayload(Payload):
    def __init__(self,*args,**kwargs):
        super(GenerateTokenPayload,self).__init__(*args,**kwargs)
        self.password= kwargs.get("password")

class GetPayload(Payload):
    def __init__(self,*args,**kwargs):
        super(GetPayload,self).__init__(*args,**kwargs)
        self.key      = kwargs.get("key",nanoid_())



class PutPayload(Payload):
    def __init__(self,*args,**kwargs):
        super(PutPayload,self).__init__(*args,**kwargs)
        self.bytes    = kwargs.get("bytes",bytearray())
        self.key      = kwargs.get("key",nanoid_())
        self.metadata = kwargs.get("metadata",{})
    def __str__(self):
        return "PutPayload(key={})".format(self.key)
    def get_hash(self)->str:
        sha256 = H.sha256()
        sha256.update(self.bytes)
        return sha256.hexdigest()

class PutNDArrayPayload(Payload):
    def __init__(self,*args,**kwargs):
        super(PutNDArrayPayload,self).__init__(*args,**kwargs)
        self.ndarray:npt.NDArray = kwargs.get("ndarray",np.array([]))
        self.key   = kwargs.get("key",nanoid_())
        self.metadata = kwargs.get("metadata",{})
    def into(self)->PutPayload:
        return PutPayload(
            bytes = self.ndarray.tobytes(), 
            key = self.key, 
            metadata = {**self.metadata, "shape": str(self.ndarray.shape),"dtype":str(self.ndarray.dtype)
                        }

        )
    

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


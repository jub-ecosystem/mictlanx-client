from abc import ABC
from nanoid import generate as nanoid_
import numpy as np
import numpy.typing as npt
import hashlib as H
from typing import TypeVar,Generic

class Payload(ABC):
    def __init__(self,*args,**kwargs):
        pass
    def to_dict(self):
        return self.__dict__

class GenerateTokenPayload(Payload):
    def __init__(self,*args,**kwargs):
        super(GenerateTokenPayload,self).__init__(*args,**kwargs)
        self.password= kwargs.get("password")
        self.expires_in = kwargs.get("expires_in",3600)

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
    

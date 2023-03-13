# import socket as S
from option import Result,Err,Ok
from mictlanx.logger.log import Log
import mictlanx.v2.interfaces.requests as Requests
import mictlanx.v2.interfaces.responses as Responses
from mictlanx.v2.codec.codec import ClientCodec
from mictlanx.v2.services.service import Service


class StorageNodeService(Service):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def get(self,**kwargs)->Result[Responses.PutResponse, Responses.ErrorResponse]:
        key     = kwargs.get("key")
        headers = kwargs.get("headers",{})
        request = Requests.Get(key = key, headers = headers )
        try:
            self.socket.sendall(request.encode())
            response = ClientCodec.decode(socket = self.socket)
            if(Responses.GetResponse.check(response)):
                return Ok(response)
            else:
                return Err(response)
        except Exception as e:
            return Err(Responses.ExceptionResponse(cause = str(e)))
    
    def put(self,**kwargs)->Result[Responses.PutResponse, Responses.ErrorResponse]:
        key     = kwargs.get("key")
        value   = kwargs.get("value")
        headers = kwargs.get("headers",{})
        request = Requests.Put(key = key, value=value, headers = headers )
        try:
            self.socket.sendall(request.encode())
            response = ClientCodec.decode(socket = self.socket)
            # print
            if(Responses.PutResponse.check(response)):
                return Ok(response)
            else:
                return Err(response)
        except Exception as e:
            return Err(Responses.ExceptionResponse(cause = str(e)))




import socket as S
from option import Result,Err,Ok
from mictlanx.logger.log import Log
import mictlanx.v2.interfaces.requests as Requests
import mictlanx.v2.interfaces.responses as Responses
from mictlanx.v2.codec.codec import ClientCodec
from mictlanx.v2.services.service import Service

class ReplicaManagerService(Service):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
    def balance(self,**kwargs)->Result[Responses.Balanced,Responses.ErrorResponse]:
        headers     = kwargs.get("headers",{})
        balance_req = Requests.Balance(headers =  headers)
        try:
            self.socket.sendall(balance_req.encode())
            response = ClientCodec.decode(socket=self.socket)
            print("BALANCE_RESPONSE",response)
            if(Responses.Balanced.check(response)):
                return Ok(response)
            else:
                return Err(response)
        except Exception as e:
            return Err(Responses.ExceptionResponse(cause = str(e)))
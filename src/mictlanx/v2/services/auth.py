import socket as S
from option import Result,Err,Ok
from mictlanx.logger.log import Log
import mictlanx.v2.interfaces.requests as Requests
import mictlanx.v2.interfaces.responses as Responses
from mictlanx.v2.codec.codec import ClientCodec
from mictlanx.v2.services.service import Service

class AuthService(Service):
    def __init__(self,*args,**kwargs):
        super(AuthService,self).__init__(*args,**kwargs)
    def generate_token(self,**kwargs)->Result[Responses.GeneratedToken,Responses.ErrorResponse]:
        client_id = kwargs.get("client_id")
        password  = kwargs.get("password")
        headers   = kwargs.get("headers",{})
        request = Requests.GenerateToken(client_id =client_id, password= password,headers= headers)
        try:
            self.socket.sendall(request.encode())
            response = ClientCodec.decode(socket=self.socket)
            if(Responses.GeneratedToken.check(response)):
                return Ok(response)
            else:
                return Err(response)
        except Exception as e:
            return Err(Responses.ExceptionResponse(cause = str(e)))

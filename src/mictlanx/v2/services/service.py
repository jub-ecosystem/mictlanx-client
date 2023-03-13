from abc import ABC
import socket as S
from option import Result,Err,Ok
from mictlanx.logger.log import Log
import mictlanx.v2.interfaces.requests as Requests
import mictlanx.v2.interfaces.responses as Responses
from mictlanx.v2.codec.codec import ClientCodec


class Service(ABC):
    def __init__(self,**kwargs):
        self.socket   = S.socket(S.AF_INET,S.SOCK_STREAM)
        self.hostname = kwargs.get("ip_addr","localhost")
        self.port     = kwargs.get("port",10000)
        self.socket.connect((self.hostname,self.port))
        self.logger = kwargs.get("logger",Log())
    def exit(self,**kwargs)->Result[Responses.Exited,Exception]:
        exit_req = Requests.Exit(**kwargs)
        self.logger.debug("EXITING...")
        try:
            self.socket.sendall(exit_req.encode())
            exit_res = ClientCodec.decode(socket = self.socket)
            self.logger.debug("EXIT_RESPONSE {}".format(exit_res))
            return Ok(exit_res)
        except Exception as e:
            return Err(Responses.ExceptionResponse(cause = str(e)))
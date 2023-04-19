from abc import ABC
import socket as S
from option import Result,Err,Ok
from mictlanx.logger.log import Log
import mictlanx.v2.interfaces.requests as Requests
import mictlanx.v2.interfaces.responses as Responses
from mictlanx.v2.codec.codec import ClientCodec
from mictlanx.logger.log import Log
from retry import retry
from uuid import uuid4
import time as T 


class Service(ABC):
    def __init__(self,**kwargs):
        self.socket        = S.socket(S.AF_INET,S.SOCK_STREAM)
        self.service_id    = kwargs.get("service_id","service-{}".format(str(uuid4())[:6] ))
        self.hostname      = kwargs.get("ip_addr","localhost")
        self.port          = kwargs.get("port",10000)
        self.logger:Log    = kwargs.get("logger")
        self.tries_counter = 0 
        self.start_time    = T.time()
        self.__connect()
    

    @retry(delay = 1,tries=100,jitter=(0,5))
    def __connect(self):
        try:
            self.logger.debug("SERVICE_CONNECTION_ATTEMPT {} {}".format(self.service_id,self.tries_counter+1))
            self.socket = S.create_connection((self.hostname, self.port), timeout = 10)
            self.logger.info("SERVICE_CONNECTION_TIME {} {} {}".format(self.service_id, T.time() - self.start_time, self.tries_counter+1))
            return 
        except Exception as e:
            self.tries_counter+=1
            self.logger.error("SERVICE_CONNECTION_ERROR {} {} {}".format(self.service_id, self.tries_counter, str(e)))
            raise e
        
    def exit(self,**kwargs)->Result[Responses.Exited,Exception]:
        exit_req = Requests.Exit(**kwargs)
        # self.logger.debug("EXITING...")
        try:
            self.socket.sendall(exit_req.encode())
            exit_res = ClientCodec.decode(socket = self.socket)
            self.logger.debug("EXIT_RESPONSE {}".format(exit_res))
            return Ok(exit_res)
        except Exception as e:
            return Err(Responses.ExceptionResponse(cause = str(e)))
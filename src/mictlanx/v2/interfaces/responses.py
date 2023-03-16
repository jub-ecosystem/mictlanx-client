from typing import Generic,TypeVar
from mictlanx.v2.codec.codec import Decoder
from mictlanx.v2.constants.constants import Constants
from mictlanx.v2.interfaces.metadata import Metadata
from mictlanx.interfaces.statues import Status
import json
import socket as S
from abc import ABC
import numpy.typing as npt


class Response(ABC):
    pass

class ErrorResponse(ABC):
    pass

class ExceptionResponse(ErrorResponse):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.cause = kwargs.get("cause")
    # def new(**kwargs):
        # return 



class Unprocessable(ErrorResponse):
    def __init__(self,**kwargs):
        self.response_id = kwargs.get("response_id")
        self.status = kwargs.get("status")
        self.description = kwargs.get("description")
        self.service_time = kwargs.get("service_time")
    def decode(**kwargs):
        socket           = kwargs.get("socket")
        status           = Decoder.decode_i8(socket=socket)
        response_id      = Decoder.decode_string(socket = socket)
        description      = Decoder.decode_string(socket = socket)
        service_time     = Decoder.decode_u128(socket=socket)
        return Unprocessable(response_id= response_id,description= description, service_time= service_time, status=status)
    def __str__(self):
        return "Unprocessable(response_id={}, description={}, service_time={})".format(self.response_id,self.description,self.service_time)
class BadRequest(ErrorResponse):
    def __init__(self,**kwargs):
        self.response_id = kwargs.get("response_id")
        self.status = kwargs.get("status")
        self.description = kwargs.get("description")
        self.service_time = kwargs.get("service_time")
    def decode(**kwargs):
        socket           = kwargs.get("socket")
        status           = Decoder.decode_i8(socket=socket)
        response_id      = Decoder.decode_string(socket = socket)
        description      = Decoder.decode_string(socket = socket)
        service_time     = Decoder.decode_u128(socket=socket)
        return BadRequest(response_id= response_id,description= description, service_time= service_time, status=status)
    def __str__(self):
        return "BadRequest(response_id={}, description={}, service_time={})".format(self.response_id,self.description,self.service_time)


class Exited(Response):
    def __init__(self,**kwargs):
        self.response_id  = kwargs.get("response_id","response-id")
        self.status       = kwargs.get("status",0)
        self.service_time = kwargs.get("service_time",0)
    def decode(**kwargs):
        socket           = kwargs.get("socket")
        status           = kwargs.get("status",Decoder.decode_i8(socket=socket))
        response_id      = Decoder.decode_string(socket = socket)
        service_time     = Decoder.decode_u128(socket=socket)
        return Exited(status = status, response_id= response_id, service_time = service_time)


class Balanced(Response):
    def __init__(self,**kwargs):
        self.status       = kwargs.get("status",0)
        self.response_id  = kwargs.get("response_id","RESPONSE_ID")
        self.node_id      = kwargs.get("node_id","NODE_ID")
        self.ip_addr      = kwargs.get("ip_addr","IP_ADDR")
        self.port         = kwargs.get("port",0)
        self.service_time = kwargs.get("service_time",0)

    def check(response)->bool:
        return type(response) == Balanced
    def __str__(self):
        return "Balanced(node_id={}, ip_addr={}, port={}, service_time={})".format(self.node_id, self.ip_addr, self.port, self.service_time)
    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status           = Decoder.decode_i8(socket=socket)
            # print("STATUS",status)
            response_id  = Decoder.decode_string(socket = socket)
            # print("RESPONSE_ID",response_id)
            node_id      = Decoder.decode_string(socket = socket)
            # print("NODE_ID",node_id)
            ip_addr      = Decoder.decode_string(socket = socket)
            # print("IP_ADDR",ip_addr)
            port         = Decoder.decode_u16(socket=socket)
            # print("PORT",port)
            service_time = Decoder.decode_u128(socket=socket)
            # print("SERVICE_TIME",service_time)
            return Balanced(
                status       = status,
                response_id  = response_id,
                node_id      = node_id,
                ip_addr      = ip_addr,
                service_time = service_time,
                port = port
            )
            # else:
                # raise Exception("NO VALID RESPONSE {}".format(status))



class GeneratedToken(Response):
    def __init__(self,**kwargs):
        self.status            = kwargs.get("status",0)
        self.response_id       = kwargs.get("response_id","")
        self.token             = kwargs.get("token","TOKEN")
        self.json_web_token_id = kwargs.get("json_web_token_id","TOKEN")
        self.service_time      = kwargs.get("service_time",0)

    def __str__(self):
        return "GenerateTokenResponse(response_id={}, token={}, json_web_token_id={}, service_time={})".format(self.response_id,self.token,self.json_web_token_id,self.service_time)
    def check(response)->bool:
        return type(response) == GeneratedToken
    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status           = Decoder.decode_i8(socket=socket)
            if(status == Status.Success):
                response_id       = Decoder.decode_string(socket = socket)
                json_web_token_id = Decoder.decode_string(socket = socket)
                token             = Decoder.decode_string(socket = socket)
                service_time      = Decoder.decode_u128(socket=socket)
                return GeneratedToken(
                    status= status,
                    response_id = response_id,
                    json_web_token_id = json_web_token_id,
                    token= token,
                    service_time=service_time
                )
            return None


class PutResponse(Response):
    # class Success(object):
    def __init__(self,**kwargs):
        self.response_id  = kwargs.get("response_id","response-id")
        self.status       = kwargs.get("status",0)
        self.response_time = kwargs.get("response_time",0)
        self.service_time = kwargs.get("service_time",0)
        self.throughput   = kwargs.get("throughput",0.0)
        self.metadata     = Metadata(**kwargs.get("metadata",{}))
    def check(response)->bool:
        return type(response) == PutResponse

    def __str__(self,**kwargs):
        return "PutResponse(response_id={}, id={}, size={}, service_time={}, throughput={},response_time={})".format(self.response_id, self.metadata.id,self.metadata.size,self.service_time,self.throughput,self.response_time)
    # class BadParameters(object):
    #     def __init__(self,**kwargs):
    #         self.status       = -5
    #         self.response_id  = kwargs.get("response_id","BAD_PARAMETERS")
    #         self.service_time = kwargs.get("service_time",0)

    # def __init__(self,**kwargs):
    #     self.response_id  = kwargs.get("response_id","response-id")
    #     self.status       = kwargs.get("status",0)
    #     self.service_time = kwargs.get("service_time",0)
    #     self.throughput   = kwargs.get("throughput",0.0)
    #     self.metadata     = Metadata(**kwargs.get("metadata",{}))
    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status           = Decoder.decode_i8(socket=socket)
            # if(status == Status.Success):
            response_id      = Decoder.decode_string(socket = socket)
            service_time     = Decoder.decode_u128(socket=socket)
            throughput       = Decoder.decode_double(socket=socket)
            metadata_str     = Decoder.decode_string(socket=socket,buf_size = Constants.U32_BYTES_SIZE)
            metadata         = json.loads(metadata_str)
            return PutResponse(
                response_id  = response_id,
                status       = status,
                service_time = service_time,
                throughput   = throughput,
                metadata     = metadata
            )
            # elif(status == Status.Updated):
            #     pass
            # elif(status == Status.Exited):
            #     return Exited.decode(socket=socket,status=Status.Exited)



class NotFound(object):
    def __init__(self,**kwargs):
        self.response_id  = kwargs.get("response_id","response-id")
        self.status       = kwargs.get("status",0)
        self.key          = kwargs.get("key","KEY")
        self.service_time = kwargs.get("service_time",0)
        # self.response_time = kwargs.get("response_time",0)
    def check(response)->bool:
        return type(response) == NotFound
    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status        = Decoder.decode_i8(socket = socket)
            # print("STATUS",status)
            # 
            response_id   = Decoder.decode_string(socket=socket)
            # print("RESPONSE_ID ", response_id)
            # 
            key   = Decoder.decode_string(socket=socket)
            # print("KEY", key)
            # 
            service_time  = Decoder.decode_u128(socket=socket)
            # print("SERVICE_TIME",service_time)
            # 
            return NotFound(status=status, response_id = response_id, key = key, service_time = service_time)
    def __str__(self):
        return "NotFound(response_id={}, key={}, service_time={})".format(self.response_id,self.key,self.service_time)


T = TypeVar("T")
# class GetResponse(Generic[T]):

class GetResponse(Generic[T]):
    # class Sucess(object):
    def __init__(self,**kwargs):
        # super.__init__(**kwargs) 
        self.response_id   = kwargs.get("response_id","response-id")
        self.status        = kwargs.get("status",0)
        self.service_time  = kwargs.get("service_time",0)
        self.throughput    = kwargs.get("throughput",0.0)
        self.response_time = kwargs.get("response_time",0)
        self.metadata      = Metadata(**kwargs.get("metadata",{}))
        self.value         = kwargs.get("value",bytes())
    
    def __str__(self):
        return "GetResponse(response_id={}, id={}, size={}, service_time={}, throughput={}, response_time={})".format(self.response_id,self.metadata.id,len(self.value),self.service_time,self.throughput,self.response_time)

    def check(response)->bool:
        return type(response) == GetResponse
    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status = Decoder.decode_i8(socket = socket)
            # if(status ==Status.Success):
            response_id   = Decoder.decode_string(socket=socket)
            # print("RESPONSE_ID ", response_id)
            # 
            service_time  = Decoder.decode_u128(socket=socket)
            # print("SERVICE_TIME",service_time)
            # 
            throughput    = Decoder.decode_double(socket=socket)
            # 
            metadata_str  = Decoder.decode_string(socket=socket, buf_size = Constants.U32_BYTES_SIZE)
            # 
            metadata      = json.loads(metadata_str)
            response      = GetResponse(response_id = response_id, status = status, service_time = service_time, throughput = throughput, metadata=metadata)
            value         = socket.recv(response.metadata.size)
            response.value = value 
            return response

class GetNDArrayResponse(GetResponse[npt.NDArray]):
    def __init__(self,**kwargs):
        super.__init__(**kwargs)
        # self.value

class GetBytesResponse(GetResponse[bytes]):
    def __init__(self,**kwargs):
        super.__init__(**kwargs)
            

class VerifiedToken(Response):
    def __init__(self,**kwargs):
        self.status            = kwargs.get("status",0)
        self.response_id       = kwargs.get("response_id","")
        self.verify            = kwargs.get("verify",False)
        self.service_time      = kwargs.get("service_time",0)

    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status           = Decoder.decode_i8(socket=socket)
            if(status == Status.Success):
                response_id        = Decoder.decode_string(socket = socket)
                # print("RESPONSE_ID",response_id)
                verify  = Decoder.decode_u8(socket = socket)
                verify  = bool(verify)
                # print("VERIFY",verify)
                service_time = Decoder.decode_u128(socket=socket)
                # print("SERVICE_TIME",service_time)
                return VerifiedToken(
                    status= status,
                    response_id = response_id,
                    verify = verify,
                    service_time=service_time
                )
            return None

class Unauthorized(ErrorResponse):
    def __init__(self,**kwargs):
        self.status            = kwargs.get("status",0)
        self.response_id       = kwargs.get("response_id","")
        # self.verify            = kwargs.get("verify",False)
        self.service_time      = kwargs.get("service_time",0)

    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status           = Decoder.decode_i8(socket=socket)
            if(status == Status.Success):
                response_id        = Decoder.decode_string(socket = socket)
                # print("RESPONSE_ID",response_id)
                # verify  = Decoder.decode_u8(socket = socket)
                # verify  = bool(verify)
                # print("VERIFY",verify)
                service_time = Decoder.decode_u128(socket=socket)
                # print("SERVICE_TIME",service_time)
                return Unauthorized(
                    status= status,
                    response_id = response_id,
                    # verify = verify,
                    service_time=service_time
                )
            return None

class NotFoundAvailableNodes(ErrorResponse):
    def __init__(self,**kwargs):
        self.status            = kwargs.get("status",0)
        self.response_id       = kwargs.get("response_id","")
        # self.verify            = kwargs.get("verify",False)
        self.service_time      = kwargs.get("service_time",0)

    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status           = Decoder.decode_i8(socket=socket)
            response_id        = Decoder.decode_string(socket = socket)
            # print("RESPONSE_ID",response_id)
            service_time = Decoder.decode_u128(socket=socket)
            # print("SERVICE_TIME",service_time)
            return NotFoundAvailableNodes(
                status= status,
                response_id = response_id,
                service_time=service_time
            )
class MaxClientReached(ErrorResponse):
    def __init__(self,**kwargs):
        self.status = kwargs.get("status",-1)
        self.response_id = kwargs.get("response_id")
    def check(response)->bool:
        return type(response) == MaxClientReached
    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        if not (socket):
            raise Exception("NO SOCKET PROVIDED")
        else:
            status           = Decoder.decode_i8(socket=socket)
            response_id        = Decoder.decode_string(socket = socket)
            # print("RESPONSE_ID",response_id)
            return MaxClientReached(status=status, response_id=response_id)


        # self.service_time = kwargs.get("service_time",0)
        


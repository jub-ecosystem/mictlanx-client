from mictlanx.v2.constants.constants import Constants
from mictlanx.v2.interfaces import responses
# PutResponse,GetResponse,GenerateTokenResponse,Balanced,VerifiedToken
import struct
import socket as S 


class ClientCodec(object):
    def __init__(self,**kwargs):
        pass
    def decode(**kwargs):
        socket:S.socket = kwargs.get("socket",None)
        try:
            if not (socket):
                raise Exception("NO SOCKET PROVIDED")
            else:
                response_type = Decoder.decode_u8(socket = socket)
                print("RESPONSE_TYPE",response_type)
                if(response_type == Constants.PUT):
                    return responses.PutResponse.decode(socket = socket)
                elif(response_type == Constants.GET): 
                    cache     = kwargs.get("cache",False)
                    sink_path = kwargs.get("sink_path","/sink/mictlanx/local")
                    return responses.GetResponse.decode(socket=socket,sink_path=sink_path,cache=cache)
                elif(response_type == Constants.EXIT):
                    return responses.Exited(socket=socket)
                elif(response_type == Constants.GENERATE_TOKEN):
                    return responses.GeneratedToken.decode(socket=socket)
                elif(response_type == Constants.VERIFIED_TOKEN):
                    return responses.VerifiedToken.decode(socket=socket)
                elif(response_type == Constants.BALANCE):
                    return responses.Balanced.decode(socket=socket)
                # _____________________________________________________
                elif(response_type == Constants.GET_METADATA):
                    return None
                elif (response_type == Constants.SET_TAGS):
                    return None
                elif(response_type == Constants.DEL):
                    return None
                elif(response_type == Constants.CHECK_INTEGRITY):
                    return None
                elif(response_type == Constants.REFRESH_TOKEN):
                    return None
                elif(response_type == Constants.DESTROY_TOKEN):
                    return None
                elif(response_type == Constants.ADD_NODE):
                    return None
                elif(response_type == Constants.LIST_NODES):
                    return None
                # ERRORS
                elif(response_type == Constants.UNPROCESSABLE):
                    return responses.Unprocessable.decode(socket=socket)
                elif(response_type == Constants.MAX_CLIENT_REACHED):
                    return responses.MaxClientReached(socket=socket)
                elif(response_type == Constants.UNAUTHORIZED):
                    return responses.Unauthorized(socket=socket)
                elif(response_type == Constants.NOT_FOUND_AVAILABLE_NODES):
                    return responses.NotFoundAvailableNodes.decode(socket=socket)
                elif(response_type == Constants.BAD_REQUEST):
                    return responses.BadRequest.decode(socket=socket)
                elif(response_type == Constants.DECODE_ERROR):
                    return None
                elif(response_type == Constants.NOT_FOUND):
                    return responses.NotFound.decode(socket=socket)
        except Exception as e:
            print("DECODE_ERROR",e)
            raise e



class Decoder(object):
    def decode_string(**kwargs):
        try:
            socket       = kwargs.get("socket")
            buf_size     = kwargs.get("buf_size",Constants.U16_BYTES_SIZE)
            if(buf_size == Constants.U16_BYTES_SIZE):
                string_size  = Decoder.decode_u16(socket=socket)
            elif(buf_size == Constants.U32_BYTES_SIZE):
                string_size  = Decoder.decode_u32(socket=socket)
            string_bytes = socket.recv(string_size)
            return string_bytes.decode("utf8")
        except Exception as e:
            raise e


    def decode_int(**kwargs):
        try:
            socket:S.socket = kwargs.get("socket")
            buf_size        = kwargs.get("buf_size",Constants.U8_BYTES_SIZE)
            signed          = kwargs.get("signed",False)
            int_bytes       = socket.recv(buf_size)
            # print("INT_BYTES",int_bytes)
            int_value       = int.from_bytes(int_bytes,"big",signed=signed)
            return int_value
        except Exception as e:
            raise e

    def decode_double(**kwargs):
        try:
            socket           = kwargs.get("socket")
            throughput_bytes = socket.recv(Constants.F64_BYTES_SIZE)
            throughput       = struct.unpack(">d",throughput_bytes)[0]
            return throughput
        except Exception as e:
            raise e

    def decode_i8(**kwargs):
        try:
            socket       = kwargs.get("socket")
            service_time = Decoder.decode_int(socket= socket, buf_size = Constants.U8_BYTES_SIZE,signed=True)
            return service_time
        except Exception as e:
            raise e
    def decode_u8(**kwargs):
        try:
            socket       = kwargs.get("socket")
            service_time = Decoder.decode_int(socket= socket, buf_size = Constants.U8_BYTES_SIZE)
            return service_time
        except Exception as e:
            raise e
    def decode_u16(**kwargs):
        try:
            socket       = kwargs.get("socket")
            service_time = Decoder.decode_int(socket= socket, buf_size = Constants.U16_BYTES_SIZE)
            return service_time
        except Exception as e:
            raise e
    def decode_u32(**kwargs):
        try:
            socket       = kwargs.get("socket")
            service_time = Decoder.decode_int(socket= socket, buf_size = Constants.U32_BYTES_SIZE)
            return service_time
        except Exception as e:
            raise e
    def decode_u128(**kwargs):
        try:
            socket       = kwargs.get("socket")
            service_time = Decoder.decode_int(socket=socket, buf_size = Constants.U128_BYTES_SIZE)
            return service_time
        except Exception as e:
            raise e
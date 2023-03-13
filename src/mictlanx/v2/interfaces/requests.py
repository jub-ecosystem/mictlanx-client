from typing import Dict
import json
from mictlanx.v2.constants.constants import Constants

class BaseEncode(object):
    def encode_headers(**kwargs):
        headers = kwargs.get("headers",{})
        dst     = kwargs.get("dst",bytearray())
        headers_str        = json.dumps(headers)
        headers_size       = len(headers_str)
        # print("HEADER_SIZE",headers_size)
        headers_size_bytes = headers_size.to_bytes(Constants.U32_BYTES_SIZE,"big")
        headers_bytes = headers_str.encode(encoding="utf8")
        dst.extend(headers_size_bytes)
        dst.extend(headers_bytes)
        return dst

    def encode_int(**kwargs):
        value:int              = kwargs.get("value",0) 
        length                 = kwargs.get("length",Constants.U8_BYTES_SIZE)
        dst                    = kwargs.get("dst",bytearray())
        value_size_bytes:bytes = value.to_bytes(length,"big")
        dst.extend(value_size_bytes)
        return dst
    def encode_u8(**kwargs):
        return BaseEncode.encode_int(**{**kwargs, "length":Constants.U8_BYTES_SIZE})
    def encode_u16(**kwargs):
        return BaseEncode.encode_int(**{**kwargs, "length":Constants.U16_BYTES_SIZE})
    def encode_u32(**kwargs):
        return BaseEncode.encode_int(**{**kwargs, "length":Constants.U32_BYTES_SIZE})
    def encode_u64(**kwargs):
        return BaseEncode.encode_int(**{**kwargs, "length":Constants.U64_BYTES_SIZE})
    def encode_u128(**kwargs):
        return BaseEncode.encode_int(**{**kwargs, "length":Constants.U128_BYTES_SIZE})
    
    def encode_str(**kwargs):
        value:str              = kwargs.get("value","") 
        dst                    = kwargs.get("dst",bytearray)
        str_buf_size           = kwargs.get("str_buf_size",Constants.U16_BYTES_SIZE)
        value_size:int         = len(value)
        value_size_bytes:bytes = value_size.to_bytes(str_buf_size,"big")
        # print("VALUE_SIZE_BYTES",value_size_bytes)
        dst.extend(value_size_bytes)
        # APPEND KEY
        dst.extend(value.encode())
        return dst



class GenerateToken(object):
    def __init__(self,**kwargs):
        self.client_id = kwargs.get("client_id","")
        self.headers   = kwargs.get("headers",{})
        self.password  = kwargs.get("password","123456")
    def encode(self)->bytearray:
        dst:bytearray = bytearray()
        # dst.extend(Constants.GENERATE_TOKEN)
        dst = BaseEncode.encode_u8(value = Constants.GENERATE_TOKEN,dst=dst)
        # Client ID
        dst = BaseEncode.encode_str(value = self.client_id, dst = dst)
        # Password
        dst = BaseEncode.encode_str(value = self.password, dst = dst)
        # HEADERS 
        dst  = BaseEncode.encode_headers(headers = self.headers, dst = dst)
        return dst

class VerifyToken(object):
    def __init__(self,**kwargs):
        self.headers   = kwargs.get("headers",{})
        self.client_id = kwargs.get("client_id","")
        self.json_web_token_id = kwargs.get("json_web_token_id","123456")
        self.token  = kwargs.get("token","123456")
    def encode(self)->bytearray:
        dst:bytearray = bytearray()
        # dst.extend(Constants.GENERATE_TOKEN)
        dst = BaseEncode.encode_u8(value = Constants.VERIFIED_TOKEN,dst=dst)
        # Client ID
        dst = BaseEncode.encode_str(value = self.client_id, dst = dst)
        # JWT ID
        dst = BaseEncode.encode_str(value = self.json_web_token_id, dst = dst)
        # TOKEN
        dst = BaseEncode.encode_str(value = self.token, dst = dst)
        # HEADERS 
        dst  = BaseEncode.encode_headers(headers = self.headers, dst = dst)
        return dst


class Balance(object):
    def __init__(self,**kwargs):
        # self.client_id = kwargs.get("client_id","")
        self.headers   = kwargs.get("headers",{})
        # self.token     = kwargs.get("token","123456")
    def encode(self)->bytearray:
        dst:bytearray = bytearray()
        dst  = BaseEncode.encode_u8(value = Constants.BALANCE,dst=dst)
        # dst  = BaseEncode.encode_str(value = self.token, dst= dst )
        dst  = BaseEncode.encode_headers(headers = self.headers, dst = dst)
        return dst

class Exit(object):
    def __init__(self,**kwargs):
        self.headers = kwargs.get("headers",{})
    def encode(self)->bytearray : 
        dst:bytearray = bytearray()
        dst = BaseEncode.encode_u8(value = Constants.EXIT,dst=dst)
        dst = BaseEncode.encode_headers(headers = self.headers,dst=dst)
        return dst

class Put(object):
    def __init__(self,**kwargs):
        self.key                   = kwargs.get("key","")
        self.value                 = kwargs.get("value",bytes())
        self.headers:Dict[str,str] = kwargs.get("headers",{})
        # self.start_time            = kwargs.get("start_time",0)

    def __process_headers(self,**kwargs):
        headers = kwargs.get("headers",{})
        if("tags" in headers):
            headers["tags"] = json.dumps(headers.get("tags",{}))
        return headers
    def update_headers(self,**kwargs):
        self.headers = {**self.headers,**kwargs}
    
    def encode(self) -> bytearray:
        dst:bytearray = bytearray()
        # APPEND CMD BYTES
        # dst.extend(Constants.PUT)
        dst = BaseEncode.encode_u8(value= Constants.PUT,dst=dst)
        # HEADERS  
        dst = BaseEncode.encode_headers(headers = self.__process_headers(headers= self.headers) , dst  = dst)
        # APPEND KEY SIZE
        dst = BaseEncode.encode_str(value = self.key, dst = dst,str_buf_size = Constants.U16_BYTES_SIZE)
        # APPEND VALUE_SIZE
        dst = BaseEncode.encode_u128(value = len(self.value), dst= dst)
        dst.extend(self.value)
        return dst
    
    # def decode(**kwargs):
    #     source:bytearray = kwargs.get("source",bytearray())
    #     cursor = 0
    #     if (len(source) == 0 ):
    #         return None
    #     else: 
    #         cmd              = source[0]
    #         cursor           +=1 
    #         key_size_bytes   =  source[cursor:Constants.KEY_BYTES_SIZE+cursor]
    #         # source.
    #         cursor           += Constants.KEY_BYTES_SIZE
    #         key_size         = int.from_bytes(key_size_bytes,"big")
    #         key_bytes        = source[cursor:key_size+cursor]
    #         cursor           += key_size
    #         key              = key_bytes.decode("utf8")
    #         value_size_bytes = source[cursor:cursor+Constants.VALUE_BYTES_SIZE]
    #         value_size       = int.from_bytes(value_size_bytes,"big")
    #         cursor           += Constants.VALUE_BYTES_SIZE
    #         value            = source[cursor:]
    #         # source.read
    #         print("CMD {}".format(cmd))
    #         print("KEY_SIZE_BYTES {}".format(key_size_bytes))
    #         print("KEY_SIZE {}".format(key_size))
    #         print("KEY_BYTE {}".format(key_bytes))
    #         print("KEY {}".format(key))
    #         print("VALUE_SIZE_BYTES {}".format(value_size_bytes))
    #         print("VALUE_SIZE {}".format(value_size))
    #         return Put(id= key,value=value)
    def __str__(self):
        return "PutResquest(key = {}, value = {})".format(self.key,self.value)
            # response_size       = int.from_bytes(response_size_bytes,"big")

class Get(object):
    def __init__(self,**kwargs):
        self.key   = kwargs.get("key","")
        self.headers = kwargs.get("headers",{})
    def encode(self) -> bytearray:
        dst:bytearray = bytearray()
        # APPEND CMD BYTES
        dst = BaseEncode.encode_u8(value=Constants.GET,dst=dst)
        # APPEND KEY SIZE
        dst = BaseEncode.encode_str(value = self.key, str_buf_size = Constants.U16_BYTES_SIZE, dst=dst)
        # HEADERS
        dst = BaseEncode.encode_headers(headers =self.headers,dst=dst)
        return dst
    def __str__(self):
        return "GetRequest(key = {})".format(self.key)

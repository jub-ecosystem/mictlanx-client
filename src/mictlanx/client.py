from typing import Any
import logging
import socket as S
from mictlanx.interfaces.parameters import PutParameters,GetParameters
from mictlanx.logger.log import Log
from mictlanx.interfaces.responses import GetResponse,PutResponse,GetNDArrayResponse,GetBytesResponse,Metadata
import os
import uuid 
import json
import hashlib
import time as T
import numpy as np
from mictlanx.interfaces.statues import Status
from mictlanx.utils.time_unit import sec_to_nanos


class Client(object):
    def __init__(self,**kwargs):
        self.client_id = str(uuid.uuid4())
        self.hostname  = kwargs.get("hostname","localhost")
        self.port      = kwargs.get("port",3000)
        LOG_PATH       = kwargs.get("LOG_PATH","/log")
        LOG_KWARGS        = kwargs.get("LOG_KWARGS",{})
        self.log_kwargs = kwargs.get("log_kwargs",{})
        self.INT_BYTES    = 1
        self.USIZE_BYTES  = 8
        self.TOKENS       = {
            "PUT":(1).to_bytes(self.INT_BYTES, "big"),
            "GET": (2).to_bytes(self.INT_BYTES, "big"),
        }
        self.disabled_log = kwargs.get("disabled_log",False)
        self.logger       = Log(name = self.client_id, level = logging.DEBUG, disabled = self.disabled_log, **self.log_kwargs)
    def __recvall(self,socket, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = socket.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data
    
    def __alltofile(self,socket, n,**kwargs):
        # Helper function to recv n bytes or return None if EOF is hit
        path = kwargs.get("path")
        # data = bytearray()
        m = hashlib.sha256()
        received =0
        with open(path,"wb") as f:
            while received < n:
                # print("RECEIVED {}".format(received))
                packet = socket.recv(n - received)
                if not packet:
                    break 
                received+= len(packet)
                m.update(packet)
                f.write(packet)
        
        return m.hexdigest()
        # return data
    
    def get_to_file(self,**kwargs)-> GetResponse[Any]:
        with S.socket(S.AF_INET,S.SOCK_STREAM) as socket:
            socket.connect((self.hostname,self.port))
            sink_path = kwargs.get("sink_path","/test/sink")
            CMD_BYTES    = self.TOKENS["GET"]
            # SEND CMD.
            socket.sendall(CMD_BYTES)
            # SEND GET-PARAMETERS.
            parameters    = kwargs.get("params",GetParameters(
                    id = kwargs.get("id"),
                    _from = kwargs.get("_from",None)
            ))
            # .to_json()
            params_value_bytes   = bytes(parameters.to_json(),encoding="utf8")
            params_size          = len(params_value_bytes)
            # SEND PARAMS SIZE 
            socket.sendall(params_size.to_bytes(self.USIZE_BYTES,"big"))
            # SEND PARAMS.
            socket.sendall(params_value_bytes)
            

            operation_status_bytes = self.__recvall(socket,self.INT_BYTES)
            # print("OPERATION_BYTES",operation_status_bytes)
            operation_status       = int.from_bytes(operation_status_bytes,"big",signed=True)
            # print("OPERATION_STATUS",operation_status)
            if(operation_status == Status.NotFound):
                return GetResponse.empty()
            else:
                value_size_bytes    = self.__recvall(socket,self.USIZE_BYTES) 
                value_size          = int.from_bytes(value_size_bytes,"big")
                # print("vALUE SIZE",value_size)
                if(value_size <= 0 ):
                    raise Exception("NO VALUE RECEIVED")
                checksum            = self.__alltofile(socket,value_size,path = "{}/{}".format(sink_path,parameters.id ))
                response_size_bytes = self.__recvall(socket,self.USIZE_BYTES) 
                response_size       = int.from_bytes(response_size_bytes,"big")
                if(response_size <= 0 ):
                    raise Exception("NO RESPONSE RECEIVED")
                response_bytes      = self.__recvall(socket,response_size)
                response_str        = response_bytes.decode("utf8")
                response            = json.loads(response_str)
                # print("RESPONSE",response)
                response["value"]   = None
                preserved_integrity = checksum == response.get("metadata",{}).get("checksum","")
                response["preserved_integrity"] = preserved_integrity
                res = GetResponse(**response)
                # print("_RESPONSEEE!",res)
                return res

    def get(self,**kwargs)->GetBytesResponse:
        start_time = T.time()
        try:
            with S.socket(S.AF_INET,S.SOCK_STREAM) as socket:
                socket.connect((self.hostname,self.port))
                CMD_BYTES    = self.TOKENS["GET"]
                # .to_bytes(self.INT_BYTES, "big")
                # SEND CMD.
                socket.sendall(CMD_BYTES)
                # SEND GET-PARAMETERS.
                parameters   = kwargs.get("params",GetParameters(
                    id    = kwargs.get("id"),
                    _from = kwargs.get("_from",None)
                ))
                params_value_bytes = bytes(parameters.to_json(),encoding="utf8")
                params_size  = len(params_value_bytes)
                # SEND PARAMS SIZE 
                socket.sendall(params_size.to_bytes(self.USIZE_BYTES,"big"))
                # SEND PARAMS
                socket.sendall(params_value_bytes)
                # READ OPERATION STATUS 
                operation_status_bytes = self.__recvall(socket,self.INT_BYTES)
                print("OPERATION_BYTES",operation_status_bytes)
                operation_status       = int.from_bytes(operation_status_bytes,"big",signed=True)
                print("OPERATION_STATUS",operation_status)
                if(operation_status == Status.NotFound):
                    return GetResponse()
                else:
                    value_size_bytes    = self.__recvall(socket,self.USIZE_BYTES) 
                    value_size          = int.from_bytes(value_size_bytes,"big")
                    if(value_size <= 0 ):
                        raise Exception("NO VALUE RECEIVED")
                    value               = self.__recvall(socket,value_size)
                    checksum            = hashlib.sha256(value).hexdigest()
                    response_size_bytes = self.__recvall(socket,self.USIZE_BYTES) 
                    response_size       = int.from_bytes(response_size_bytes,"big")
                    if(response_size <= 0 ):
                        raise Exception("NO RESPONSE RECEIVED")
                    response_bytes      = self.__recvall(socket,response_size)
                    response_str        = response_bytes.decode("utf8")
                    response            = json.loads(response_str)
                    response["value"]   = value
                    preserved_integrity = checksum == response.get("metadata",{}).get("checksum","")
                    response["preserved_integrity"] = preserved_integrity
                    response_time = T.time() - start_time
                    res = GetResponse(**response)
                    self.logger.info("GET {} {} {} {} {} {}".format(
                        res.id,
                        res.metadata.id,
                        res.metadata.size,
                        res.service_time,
                        res.throughput,
                        sec_to_nanos(response_time))
                    )
                    return res
        except Exception as e:
            self.logger.error(str(e))
            raise e

    def get_ndarray(self,**kwargs)->GetNDArrayResponse:
        try:

            start_time    = T.time()
            delete_file   = kwargs.get("delete",True)
            sink_path     = kwargs.get("sink_path","/sink")
            # Get metadata and bytes
            response      = self.get_to_file(**kwargs)
            # self.logger.debug(response)
            # self.logger.debug("RESPONSE_TOFILE"+str(response))
            # print("STATUS",response.status,type(response.status),response.status<0)
            if(response.status < 0):
                return response
            else:
                # Extract tags (shape and dtype)
                tags          = response.metadata.tags
                tag_keys = tags.keys()
                if not "shape" in tag_keys:
                    raise Exception("shape not found in tags")
                elif not "dtype" in tag_keys:
                    raise Exception("dtype not found in tags")
                else:
                    # Interpret shape 
                    shape         = eval(tags["shape"])
                    # Extract dtype
                    dtype         = tags["dtype"]
                    # Get matrix using bytes, shape and dtype
                    path          = "{}/{}".format(sink_path,response.metadata.id)
                    matrix        = np.fromfile(path,dtype=dtype).reshape(shape)
                    predicate     = delete_file 
                    if(predicate):
                        try:
                            os.remove(path)
                        except Exception as e :
                            raise e
                    
                    response_time = T.time() - start_time
                    self.logger.info("GET {} {} {} {} {} {}".format(
                        response.id,
                        response.metadata.id,
                        response.metadata.size,
                        response.service_time,
                        response.throughput,
                        sec_to_nanos(response_time))
                    )
                    response.value = matrix
                    return response
        except Exception as e: 
            self.logger.error(str(e))
            raise e

        
    def put(self,**kwargs)->PutResponse:
        start_time = T.time()
        with S.socket(S.AF_INET,S.SOCK_STREAM) as socket:
            try:
                socket.connect((self.hostname,self.port))
                _bytes            = kwargs.get("_bytes",[])
                parameters        = kwargs.get("parameters",
                    PutParameters(
                        id = "ball-{}".format(str(uuid.uuid4())[:4] ),
                        size = len(_bytes),
                        client_id = self.client_id 
                    )
                )
                # _______________________________________________
                CMD_BYTES          = self.TOKENS["PUT"]
                # print(parameters)
                # print(parameters.to_json())
                params_value_bytes = bytes(parameters.to_json(),encoding="utf8")
                params_size        = len(params_value_bytes)
                params_size_bytes  = params_size.to_bytes(self.USIZE_BYTES,"big")
                # SEND CMD.
                socket.sendall(CMD_BYTES)
                # SEND PARAMS SIZE.
                socket.sendall(params_size_bytes)
                # SEND PARAMS.
                socket.sendall(params_value_bytes)
                # SEND BYTES.
                socket.sendall(_bytes)
                # READ RESPONSE BYTES.
                # ____________________________________
                response_size_bytes = self.__recvall(socket,self.USIZE_BYTES)
                response_size       = int.from_bytes(response_size_bytes,"big")
                # READ RESPONSE AN DECODE AS STRING.
                response_bytes      = self.__recvall(socket,response_size).decode("utf8")
                response_json       = json.loads(response_bytes)
                # print(response_json)
                res                 = PutResponse(**response_json)
                # print(res.metadata)
                response_time       = T.time() - start_time
                self.logger.info("PUT {} {} {} {} {} {}".format(
                    res.id,
                    res.metadata.id,
                    res.metadata.size,
                    res.service_time,
                    res.throughput,
                    sec_to_nanos(response_time))
                )
                # self.logger.info("PUT {} {} {} {} {} {}".format(response.id,parameters.id,parameters.size,response,response_time))
                return  res
                # _______________________________________________
            except Exception as e:
                print(e)
                raise e
    

    def put_plot(self,**kwargs):
        try:
            plot_bytes = kwargs.get("plot_bytes")
            format             = kwargs.get("format","PNG")
            tags               = kwargs.get("tags",{})
            _bytes             = plot_bytes.getvalue()
            plot_id_suffix     = str(uuid.uuid4())[:4]
            checksum           = hashlib.sha256(_bytes).hexdigest()
            put_parmeters    = PutParameters(
                id        = kwargs.get("id","plot-{}".format(plot_id_suffix )), 
                size      = len(_bytes),
                client_id = self.client_id,
                checksum  = checksum,
                tags      = {
                    **{
                        "format":str(format),
                    },
                    **tags
                }
            )
            return self.put(
                _bytes = _bytes,
                parameters = put_parmeters,
            )
        except Exception as e:
            print(e)
            raise e

    def put_ndarray(self,**kwargs):
        try:
            matrix           = kwargs.get("matrix",np.array([]))
            _bytes           = matrix.tobytes()
            matrix_id_suffix = str(uuid.uuid4())[:4]
            checksum         = hashlib.sha256(_bytes).hexdigest()
            print("LOCAL_CHECKSUM {}".format(checksum))
            # self.__hash(_bytes)
            put_parmeters    = PutParameters(
                id        = kwargs.get("id","matrix-{}".format(matrix_id_suffix )), 
                size      = len(_bytes),
                client_id = self.client_id,
                checksum  = checksum,
                force     = kwargs.get("force",True),
                tags      = {
                    "dtype":str(matrix.dtype) ,
                    "shape": str(matrix.shape),
                    **kwargs.get("tags",{}) 
                }
            )
            
            return self.put(
                _bytes = _bytes,
                parameters = put_parmeters,
            )

        except Exception as e :
            self.logger.error(str(e))
            raise e

            

            # s.sendall(b"WRITE")






        




# class ClientV2(object):
#     # q:Queue        = Queue(maxsize=0)
#     lock           = Lock()
#     def __init__(self,**kwargs):
#         self.client_id_size = kwargs.get("client_id_size",6)
#         self.client_id      = '{}-{}'.format('client',str(uuid.uuid4())[:self.client_id_size])
#         self.hostname       = kwargs.get("hostname","localhost")
#         self.port           = kwargs.get("port",6666)
#         LOG_PATH            = kwargs.get("LOG_PATH","/log")
#         LOG_KWARGS          = kwargs.get("LOG_KWARGS",{})
#         self.debug          = kwargs.get("debug",True)
#         logger              = create_logger(LOG_PATH = LOG_PATH, LOG_FILENAME = "mictlanx-client", **LOG_KWARGS)
#         self.logger         = logger if(self.debug) else DumbLogger()
#         self.socket         = S.socket(S.AF_INET,S.SOCK_STREAM)
#         self.socket.setblocking(True)
#         self.socket.connect((self.hostname,self.port))
#     def __enter__(self):
#         return self
#     def __exit__(self,a,b,c):
#         return

    

#     def put(self,**kwargs)-> PutResponse:
#         start_time        = T.time()
#         put_request       = PutRequest(**kwargs)
#         # self.q.put(put_request)
#         H                 = hashlib.sha256()
#         H.update(put_request.value)
#         checksum          = H.digest().hex()
#         put_request.update_headers(checksum = checksum)
#         hash_service_time = sec_to_nanos(T.time() - start_time)
#         self.logger.info("HASH_TIME {} {}".format(put_request.key,hash_service_time))
#         # with S.socket(S.AF_INET,S.SOCK_STREAM) as socket:
#         # with ClientV2.lock:
#             # socket = self.socket
#         try:
#             self.socket.sendall(put_request.encode())
#             response = PutResponseV2.decode(socket = self.socket)
#             return response
#         except Exception as e: 
#             self.logger.error("{}".format(e))
#             raise e
                
#     def get(self,**kwargs):
#         self.lock.acquire()
#         start_time = T.time()
#         get_request = GetRequest(**kwargs)
#         socket = self.socket
#         # with S.socket(S.AF_INET,S.SOCK_STREAM) as socket:
#         try:
#             # socket.connect((self.hostname,self.port))
#             socket.sendall(get_request.encode())
#             response = GetResponseV2.decode(socket = socket)
#             response.response_time = sec_to_nanos(T.time() - start_time)
#             self.lock.release()
#             return response
#         except Exception as e: 
#             self.lock.release()
#             self.logger.error("{}".format(e))
#             raise e




if __name__ == "__main__":
    pass
    # import numpy as np
    # D            = np.ones((10,10))

    # LOG_KWARGS = {
    #     'console_handler_filter': lambda x:  x.levelno == logging.DEBUG or x.levelno == logging.INFO or x.levelno==logging.ERROR,
    #     'console_handler_level': logging.DEBUG
    # }
    

    # client_kwargs = {"hostname":"localhost", "port":6001, "LOG_KWARGS": LOG_KWARGS }
    # with ClientV2(**client_kwargs) as c1: 
    #     for i in range(2):
    #         put_response = c1.put(id = "matrix-{}".format(i), value = D.tobytes(), headers = {"test":"test_header"})
    #         print("Request[{}] {}".format(i,put_response))
    #         T.sleep(5)

    # del c1
    # c1 = None
    # print(put_response)
    # x = c1.get(id = "matrix-{}".format(i))
    # print(x)
    # put= PutRequest(id = "matrix-0",value = D.tobytes() )
    # get = GetRequest(id = "matrix-0")
    # encoded_put = put.encode()
    # encoeded_get = get.encode()
    # print(encoeded_get)
    # decoded_get = GetRequest.decode(source= encoeded_get)
    # print(decoded_get)

    # decode_put = PutRequest.decode(source = encoded_put)
    # print(decode_put)
    # # Establish a connection to the master storage node of the MSS on the localhost:6001.
    # c1           = Client(
    #     hostname ="localhost",
    #     port=6001, 
    #     LOG_KWARGS = {
    #         'console_handler_filter': lambda x:  x.levelno == logging.DEBUG or x.levelno == logging.INFO,
    #         'console_handler_level': logging.DEBUG
    #     }
    # )
    
    # x = c1.get_ndarray(id="matrix-0",sink_path ="/sink",delete=True)
    # print(x)
    # x.value
    
    # x.value.


    # Put a matrix using the "matrix-0" unique identifier. 
    # put_response = c1.put_ndarray(id ="matrix-0",matrix = D,force=True)
    # print(put_response)
    # GET
    # for i in range(1000):
    # print(x)
    # print(x.metadata)
    # print(x.metadata)
    # Get the matrix with the "matrix-0" identifier stored temporarily in /sink (delete flag must be set to True) .
    # metadata,matrix = c1.get_matrix(id ="matrix-0",sink_path="/sink/mictlanx", delete = False) 
    # print(matrix)
    # print(metadata)
#     c1 = Client(
#         hostname = "localhost",
#         port = 6001
#     )
    # plt.figure()
    # plt.plot([1, 2])
    # plt.title("test")
    # buf = BytesIO()
    # plt.savefig(buf, format='png')
    # c1.put_plot(plot_bytes = buf)
    # res = c1.get(id = "plot-920b")
    # print(res)



    # big_matrix = pd.read_csv("/test/experiments/source/batch1/raisin.csv").values
    
    # big_matrix = np.random.random((625,4,3))
    # c1.put_matrix(id = "matrix-1",matrix = np.array(big_matrix.tolist()))
    # big_matrix = np.ones((625,625,4))
    
    # big_matrix = np.zeros((625,625,4),dtype="float64")
    # c1.put_matrix(id = "matrix-1",matrix = big_matrix)
    # res = c1.get_matrix(id="matrix-1")
    # print(res[0])
    # print(res)
    
    # res = c1.get_to_file(id = "matrix-0",sink_path = "/test/sink")
    # res = c1.get_matrix(id = "matrix-0",sink_path = "/test/sink")
    # print(res)
    # print("_"*40)
    # start = time.time()

    # for i in range(10):
    #     c1.get_matrix(id = "matrix-0")
    #     print("_"*40)
    # st = time.time() - start
    # print("EXECUTION_TIME ",st)
    # c1.get(id = "matrix-0")
    # print("_"*40)
    # c1.get(id = "matrix-0")
    # print("_"*40)
    # c1.get(id = "matrix-0")
    # print("_"*40)
    # c1.get(id = "matrix-0")
    # c1.get(id = "matrix-ba63")
    # c1.get(id = "matrix-ba63")
    # c1.put(_bytes   = b"n")
    # put_0 = c1.put_matrix(matrix = np.array([[1,2],[3,4]]))
    # print(put_0)
    # c1.put_matrix(id = "matrix-1",matrix = big_matrix)
    # c1.put_matrix(id = "matrix-2",matrix = big_matrix)
    # c1.put_matrix(matrix = big_matrix)

from option import Result,Err,Ok
import time as T
from mictlanx.v2.client import Client
import mictlanx.v2.interfaces.responses as Responses
import numpy as np
from mictlanx.logger.log import Log
import logging 

log = Log(
    name = "c1",
    output_path ="/log/c1.log",
    error_output_path="/log/c1-error.log",
    console_handler_filter = lambda record: record.levelno == logging.DEBUG or record.levelno == logging.INFO or record.levelno == logging.ERROR,
)
c1 = Client(auth_hostname ="148.247.202.72", rm_hostname = "148.247.202.72", password="shanel", sink_path="/sink/mictlanx/local" , log = log)
# c1 = Client(auth_hostname ="localhost", rm_hostname = "localhost", password="t0p53cR3t#", sink_path="/sink/mictlanx/local" , log = log)
# c1 = Client(auth_hostname ="172.23.151.140", rm_hostname = "172.23.151.140", password="t0p53cR3t#", sink_path="/sink/mictlanx/local" , log = log)

def get(key):
    get_result = c1.get_ndarray(key=key,cache=False, force= True)
    
    if(get_result.is_ok):
        get_resppnse = get_result.unwrap()
        print("MATRIX",get_resppnse.value)
        print("GET_RESPONSE",get_resppnse)
    else:
        error = get_result.unwrap_err()
        print("GET_ERROR",error)


def process_put(x:Responses.PutResponse,max_gets:int):
    print("PUT_RESPONSE {} {}".format(x,max_gets))
    for i in range (max_gets):
        result = get(x.metadata.id)
        print("GET[{}] {}".format(i, result))

def test1():
    MAX_FILES = 10
    MAX_GETS  = 2
    for i in range(MAX_FILES):
        N        =  np.random.randint(5,100)
        M        = np.random.randint(5,100)
        matrix_0 = np.random.random(size=(N,M))
        key      = "ball-{}".format(i)
        result = get(key)
        # result = c1.put_ndarray(key =key,ndarray = matrix_0)
        # .flatmap(lambda x: process_put(x,np.random.randint(1,MAX_GETS)))
        print("RESULT",result)
        T.sleep(2)
        # if(put_result.is_ok):
        #     put_response = put_result.unwrap()
        #     print("PUT_RESPONSE [{}]".format(i),put_response)
        #     MAX_GETS=2
        #     for j in range(MAX_GETS):
        #         get(key)
        #         T.sleep(1)
        #     T.sleep(5)
        # else:
        #     error = put_result.unwrap_err()
        #     print("PUT_ERROR",error)
    # if(result_pu.is_ok):
    #     response = result.unwrap()
    #     print("RESPONSE",response)
    # else:
    #     error = result.unwrap_err()
    #     print("ERROR",error)

    # disconnected client and services..
    c1.shutdown()


test1()
# get("ball-5")

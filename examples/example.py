from option import Result,Err,Ok
import time as T
from mictlanx.v2.client import Client
import numpy as np

c1 = Client(password="shanel")

def get(key):
    get_result = c1.get_ndarray(key=key)
    if(get_result.is_ok):
        get_resppnse = get_result.unwrap()
        print("MATRIX",get_resppnse.value)
        print("GET_RESPONSE",get_resppnse)
    else:
        error = get_result.unwrap_err()
        print("GET_ERROR",error)

def test1():
    matrix_0 = np.ones((10,10))
    key = "ball-xxx"
    put_result = c1.put_ndarray(key =key,ndarray = matrix_0)
    T.sleep(1)
    if(put_result.is_ok):
        put_response = put_result.unwrap()
        print("PUT_RESPONSE",put_response)
        MAX_GETS=20
        for i in range(MAX_GETS):
            get(key)
            # get_result = c1.get_ndarray(key=key)
            # if(get_result.is_ok):
            #     get_resppnse = get_result.unwrap()
            #     print("MATRIX",get_resppnse.value)
            #     print("GET_RESPONSE",get_resppnse)
            # else:
            #     error = get_result.unwrap_err()
            #     print("GET_ERROR",error)
            T.sleep(1)

    # New client socket_address=127.0.0.1:53786
    else:
        error = put_result.unwrap_err()
        print("PUT_ERROR",error)
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

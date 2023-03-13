from option import Result,Err,Ok
import time as T
from mictlanx.v2.client import Client
import numpy as np

c1 = Client(password="shanel")
matrix_0 = np.ones((100,100))
key = "ball-1"
put_result = c1.put_ndarray(key =key,ndarray = matrix_0)
T.sleep(1)
if(put_result.is_ok):
    put_response = put_result.unwrap()
    print("PUT_RESPONSE",put_response)
    MAX_GETS=10
    for i in range(MAX_GETS):
        get_result = c1.get(key=key)
        if(get_result.is_ok):
            get_resppnse = get_result.unwrap()
            print("GET_RESPONSE",get_resppnse)
        else:
            error = get_result.unwrap_err()
            print("GET_ERROR",error)
        T.sleep(1)


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

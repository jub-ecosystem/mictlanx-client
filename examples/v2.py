from mictlanx.v2.client import Client
import numpy as np
import logging
import time as T

if __name__ == "__main__":
    LOG_KWARGS = {
        'console_handler_filter': lambda x:  x.levelno == logging.DEBUG or x.levelno == logging.INFO or x.levelno==logging.ERROR,
        'console_handler_level': logging.DEBUG
    }
    D            = np.ones((10,10))
    client_kwargs = {"hostname":"localhost", "port":6001, "LOG_KWARGS": LOG_KWARGS }
    with Client(**client_kwargs) as c1: 
        # for i in range(1):
            i =0
            # while True:
            #     i +=1
            matrix_id    =  "matrix-{}".format(i)
            put_response = c1.put_ndarray(
                id =matrix_id,
                ndarray = D,
                headers={"test_header":"TEST_HEADER_VALUE","tags":{"tag1":"tag_value"}}
            )
            print("_"*20)
            #     T.sleep(0.5)
            # print("Request[{}] {}".format(i,put_response))
            # with Client(**client_kwargs) as c2:
            T.sleep(1000)
            # get_response = c1.get_ndarray(id = matrix_id, headers= {"test":"TEST_HEADER"})
            # print("Request GET[{}] {}".format(i,get_response))
            # print("MATRIX",get_response.value)
            # print("_"*20)
            # T.sleep(5)
            # exited_res = c1.exit()
            # print("Request EXIT[{}] {}".format(i,exited_res))
            # print("_"*20)
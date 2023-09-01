from option import Some,NONE,Result,Ok,Option,Err
from typing import List,Dict
import requests as R
import hashlib as H
import time as T
from mictlanx.v4.interfaces.responses import PutMetadataResponse,PutDataResponse,PutResponse
from mictlanx.logger.log import Log
import logging as L
import magic as M
from threading import Thread
import numpy as np
import numpy.typing as npt
from concurrent.futures import ThreadPoolExecutor
import os
API_VERSION = 4 
class Client(object):
    def __init__(self,client_id:str,storage_nodes:List[str] = [], debug:bool=True,daemon:bool=True,max_workers:int = 3):
        self.__put_counter = 0
        self.__get_counter = 0
        self.frequencies   = {}
        self.storage_nodes = storage_nodes
        self.client_id = client_id
        self.debug = debug
        self.log         = Log(
            name = self.client_id,
            console_handler_filter = lambda record: self.debug
        )
        self.last_interarrival_time = 0
        self.arrival_time_sum:int = 0
        self.daemon = daemon

        if self.daemon:
            self.thread = Thread(target=self.__run,name="MictlanX")
            self.thread.setDaemon(True)
            
            self.thread.start()
        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers
        # self.
        # print("max_workers",max_workers)
        self.thread_pool = ThreadPoolExecutor(max_workers= max_workers)


    def __global_operation_counter(self):
        return self.__get_counter + self.__put_counter
    def __lb(self,)->str:
        return self.storage_nodes[self.__global_operation_counter() % len(self.storage_nodes)]

    def shutdown(self):
        self.thread_pool.shutdown(wait=True)

    def __run(self):
        while True:
            global_counter = self.__put_counter + self.__get_counter 
            if global_counter >0:
                avg_at = self.arrival_time_sum / (global_counter)
            else:
                avg_at = 0
            
            if self.debug:
                title = " ┬┴┬┴┤┬┴┬┴┤ MictlanX - Daemon ►_◄  ┬┴┬┴┤┬┴┬┴┤"
                print("|{:^50}|".format(title))
                print("-"*52)
                print(f"| {'PUT_COUNTER ':<43}| {'{}'.format(self.__put_counter):<4}|")
                print(f"| {'GET_COUNTER ':<43}| {'{}'.format(self.__get_counter):<4}|")
                print(f"| {'GLOBAL_COUNTER ':<43}| {'{}'.format(global_counter):<4}|")
                avg_at_formatted = "{:.2f}".format(avg_at)
                print(f"| {'MEAN_INTERARRIVAL ':<43}| {'{}'.format(avg_at_formatted):<4}|")
                print("-" * 52)
            T.sleep(5)

    def get_ndarray(self, key:str):
        try:
            pass
        except R.RequestException as e:
            pass
        except Exception as e:
            pass
    def get(self, key:str):
        try:
            storage_node_id = ""
            response        = R.get("http://{}/api/v{}".format(storage_node_id, API_VERSION))
        except R.RequestException as e:
            pass
        except Exception as e:
            pass

    def put_ndarray(self, key:str, ndarray:npt.NDArray,tags:Dict[str,str])->Result[PutResponse,Exception]:
        try:
            value:bytes = ndarray.tobytes()
            dtype       = str(ndarray.dtype)
            shape_str   = str(ndarray.shape)
            return self.put(key=key, value=value,tags={**tags,"dtype":dtype,"shape":shape_str })
        except R.RequestException as e:
            headers = e.response.headers | {}
            error_msg = headers.get("error-message","UKNOWN_ERROR")
            self.log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.log.error(str(e))
            return Err(e)


    def put(self,value:bytes,tags:Dict[str,str]={},timeout:int = 60, checksum_as_key:bool=False,key:str="")->Result[PutResponse,Exception]:
        x = self.thread_pool.submit(self.__put,key=key, value=value, tags=tags,checksum_as_key=checksum_as_key)
        return x.result(timeout=timeout)

    def __put(self,value:bytes,tags:Dict[str,str]={},checksum_as_key=False,key:str="")->Result[PutResponse,Exception]:
        try:
            start_time = T.time()
            if self.last_interarrival_time == 0:
                self.last_interarrival_time = start_time
            else:
                self.arrival_time_sum += (start_time - self.last_interarrival_time )
                self.last_interarrival_time = start_time

            # storage_node = self.storage_nodes[(self.__put_counter + self.__get_counter) % len(self.storage_nodes) ]
            storage_node = self.__lb()
            print("Selected storage node",storage_node)
            h = H.sha256()
            h.update(value)
            checksum:str = h.hexdigest()
            key = key if (not checksum_as_key or not key =="")  else checksum
            content_type        = M.from_buffer(value,mime=True)
            size = len(value)
            put_metadata_response =R.post("http://{}/api/v{}/metadata".format(storage_node,API_VERSION),json={
                "key":key,
                "size":size,
                "checksum":checksum,
                "tags":tags,
                "producer_id":self.client_id,
                "content_type":content_type,
            })
            put_metadata_response.raise_for_status()
            put_metadata_response = PutMetadataResponse(**put_metadata_response.json())
            
            put_response = R.post(
                "http://{}/api/v{}/data/{}".format(storage_node, API_VERSION,put_metadata_response.task_id),
                files= {
                    "upload":(key,value,content_type)
                },
                )
            put_response.raise_for_status()
            put_data_response = PutDataResponse(**put_response.json())
            response_time = T.time() - start_time
            self.log.info("{} {} {} {}".format("PUT",key,size,response_time ))
            self.__put_counter += 1
            res = PutResponse(response_time=response_time,throughput= float(size) / float(response_time))
            return Ok(res)
            # print("PUT_RESPONSE",put_data_response.service_time+put_metadata_response.service_time,put_data_response.throughput)
            
        except R.RequestException as e:
            headers = e.response.headers | {}
            error_msg = headers.get("error-message","UKNOWN_ERROR")
            self.log.error("{}".format(error_msg))
            return Err(e)
        except Exception as e:
            self.log.error(str(e))
            return Err(e)


if __name__ =="__main__":
    c=  Client(
        client_id="client-0",
        storage_nodes= ["localhost:7000","localhost:7001"],
        debug= True,
        daemon=True,
        max_workers=2
    )
    # with open("/source/01.pdf","rb") as f:
        # c.put(key="",value=f.read(),checksum_as_key=True)
    # T.sleep(10)
    for i in range(100):
        ndarray = np.random.randint(0,100,size=(10000,100))
        res = c.put_ndarray(key="matrix-{}".format(i),ndarray=ndarray,tags={"aa":"aaaa"})
        # value = b"HOLAAAAAAAAAAAAAAAAAAaa"
        # res = c.put(key="b{}".format(i),value=value, tags={})
        print(res)
        T.sleep(1)
    T.sleep(5)
    c.shutdown()
import sys
import os
import json as J
import time as T
from ipcqueue import posixmq
from threading import Thread,Event
from typing import List,Dict,Tuple
from multiprocessing.shared_memory import SharedMemory
from mictlanx.v4.interfaces.index import Ball
from mictlanx.logger.log import Log
from mictlanx.v4.xolo.utils import Utils as XoloUtils
from mictlanx.utils.index import Utils
from durations import Duration
import hashlib as H
from humanfriendly import parse_size,format_size
import logging
from dataclasses import dataclass


@dataclass
class DistributionSchemaItem:
    offset:int
    size:int
    checksum:str

class Server(object):
    def __init__(
            self,
            max_memory:str = "512MB",
            chunk_size:str = "1MB",
            heartbeat_interval:float = 1.0,
            
        ):
        self.__done_event = Event()
        self.__queue_id                               = "mictlanx"
        self.__q                                      = posixmq.Queue(
            name= '/{}'.format(self.__queue_id),
            maxsize=10, 
            maxmsgsize= 1024
        )
        self.__client_queues:Dict[str, posixmq.Queue] = {}
        self.__total_memory                           = parse_size(max_memory)
        self.__shared_memory                          = SharedMemory(
            name   = "/mictlanx-shm",
            create = True, 
            size   = self.__total_memory 
        )
        self.__distribution_schema:Dict[str,DistributionSchemaItem] = {}
        self.__chunk_size      = parse_size(chunk_size)
        self.__offset          = 0 
        self.__log             = Log(
            name = "mictlanx-server-0",
            path = "/mictlanx/client",
            console_handler_filter= lambda record: record.levelno == logging.DEBUG or record.levelno == logging.INFO or record.levelno == logging.ERROR
        )

        self.balls:List[Ball] = []
        self.__heartbeat_interval = heartbeat_interval
        # ________________________
        self.__used_memory    = 0
        self.__is_running     = True
        self.__daemon_thread  = Thread(target=self.__run,name="mictlanx",daemon=False)
    
    
    def start(self):
        self.__log.debug({
            "event":"STARTING",
            "id":self.__queue_id,
            "chunk_size":format_size(self.__chunk_size),
            "total_memory":format_size(self.__total_memory)
        })
        self.__daemon_thread.start()
        self.__done_event.wait()

    def __run(self):
        try:
            while self.__is_running:
                T.sleep(self.__heartbeat_interval)
                qsize = self.__q.qsize()
                start_time = T.time()
                if qsize == 0:
                    self.__log.debug({
                        "arrival_time":start_time,
                        "event":"HEALTH",
                        "used_memory": self.__used_memory,
                        "available_memory": self.__total_memory - self.__used_memory,
                        "total_memory": self.__total_memory,
                        "memory_uf":Utils.calculate_disk_uf(total=self.__total_memory,used= self.__used_memory, size=0),
                        "offset":self.__offset
                    })
                    continue
                request         = self.__q.get_nowait()
                client_id       = request.get("client_id","")
                key             = request.get("key","")
                path            = request.get("path","")
                operation_type  = str(request.get("operation_type","")).upper()
                task_id         = request.get("task_id","")
                
                # Write in shared memory
                if  operation_type =="" or task_id =="" or client_id == "":
                    service_time = T.time() - start_time
                    event = {
                        "client_id":client_id,
                        "task_id":task_id,
                        "key":key,
                        "path": path,
                        "operation_type":operation_type,
                        "status":-1,
                        "msg": "Bad params: ".format(path),
                        "service_time":service_time
                    }
                    self.__log.error(event)
                    # client_queue.put_nowait(event)
                    continue
                    # print("SKIP MESSAGE NO QUEUE_ID PARAM")
                else:
                    # Check if the queue is already in the local queues
                    if not client_id in self.__client_queues:
                        self.__client_queues[client_id] = posixmq.Queue(name="/{}".format(client_id))
                        client_queue = self.__client_queues[client_id]
                    else:
                        client_queue = self.__client_queues[client_id]


                    if operation_type == "PUT":
                        if not os.path.exists(path=path):
                            service_time = T.time() - start_time
                            event = {
                                "arrival_time":start_time,
                                "client_id":client_id,
                                "task_id":task_id,
                                "key":key,
                                "path": path,
                                "operation_type":operation_type,
                                "status":-1,
                                "msg": "{} does not exists in the host filesystem".format(path),
                                "service_time":service_time
                            }
                            self.__log.error(event)
                            client_queue.put_nowait(event)
                            continue
                        checksum,size   = XoloUtils.sha256_file(path=path)
                        # Check if exists
                        if checksum in self.__distribution_schema or key in self.__distribution_schema:
                            service_time = T.time() - start_time
                            event = {
                                "arrival_time":start_time,
                                "client_id":client_id,
                                "task_id":task_id,
                                "key":key,
                                "path": path,
                                "operation_type":operation_type,
                                "status":-1,
                                "msg": "{}/{} already exists in memory".format(key,checksum),
                                "service_time":service_time
                            }
                            self.__log.error(event)
                            client_queue.put_nowait(event)
                            continue
                        _key = checksum if key=="" else key
                        if self.__offset + size  <=self.__total_memory:
                            hasher = H.sha256()
                            with open(path, "rb") as f:
                                current_read_bytes = 0
                                left_bytes         = size - current_read_bytes
                                i                  = 0 

                                # self.__offset      += len(chunk_size)
                                
                                
                                while current_read_bytes < size:
                                    chunk_start_time = T.time()
                                    if left_bytes < self.__chunk_size:
                                        self.__log.debug({"msg":"{} is too small to create a chunk of size {}".format(left_bytes,format_size(self.__chunk_size))})
                                        chunk          = f.read()
                                    else:
                                        chunk = f.read(self.__chunk_size)
                                        
                                    hasher.update(chunk)
                                    chunk_size     = len(chunk)
                                    chunk_checksum = XoloUtils.sha256(value=chunk)
                                    current_read_bytes += chunk_size
                                    # self.__shared_memory.buf[(self.__offset + chunk_size*i):self.__offset + current_read_bytes] = chunk
                                    self.__shared_memory.buf[self.__offset: self.__offset + chunk_size] = chunk
                                    service_time = T.time()- start_time
                                    self.__log.debug({
                                        "arrival_time":start_time,
                                        "operation_type":"WRITE_CHUNK",
                                        "start_offset":self.__offset,
                                        "end_offset": self.__offset+chunk_size,
                                        "checksum":chunk_checksum,
                                        "chunk_size":chunk_size,
                                        "current_read_byte":current_read_bytes,
                                        "service_time":service_time
                                    })
                                    
                                    self.__offset      += chunk_size
                                    self.__used_memory += chunk_size
                                    i+=1
                                    # T.sleep(10)
                            local_checksum = hasher.hexdigest()
                            if not local_checksum == checksum:
                                service_time = T.time() - start_time
                                event = {
                                    "arrival_time":start_time,
                                    "client_id":client_id,
                                    "task_id":task_id,
                                    "key":key,
                                    "path": path,
                                    "operation_type":operation_type,
                                    "status":-1,
                                    "checksum":checksum,
                                    "_checkcsum":local_checksum,
                                    "msg":"Integrity issue: {} != {}".format(checksum,local_checksum),
                                    "service_time":service_time
                                }
                                self.__log.error(event)
                                client_queue.put_nowait(event)
                                continue
                            

                            self.__distribution_schema[_key] = DistributionSchemaItem(offset=self.__offset-size, size=size, checksum=checksum)
                            # COMPLETED
                            
                            service_time = T.time() - start_time
                            event = {
                                "arrival_time":start_time,
                                "client_id":client_id,
                                "task_id":task_id,
                                "key":_key,
                                "path": path,
                                "operation_type":operation_type,
                                "checksum":checksum,
                                "offset":self.__offset-size,
                                "size":size,
                                "status":0,
                                "service_time":service_time
                            }
                            self.__log.info(event)
                            client_queue.put_nowait(event)
                        else:
                            service_time = T.time() - start_time
                            event = {
                                "arrival_time":start_time,
                                "client_id":client_id,
                                "task_id":task_id,
                                "key":_key,
                                "path": path,
                                "operation_type":operation_type,
                                "status":-1,
                                "msg": "Run out of memory".format(path),
                                "service_time":service_time
                            }
                            self.__log.error(event)
                            client_queue.put_nowait(event)
                            continue
                    elif operation_type =="GET":
                        
                        if key in self.__distribution_schema:
                            dsi = self.__distribution_schema[key]
                            print(dsi)
                            service_time = T.time() - start_time
                            event = {
                                "arrival_time":start_time,
                                "client_id":client_id,
                                "task_id":task_id,
                                "key": key,
                                "path": path,
                                "operation_type":operation_type,
                                "service_time":service_time,
                                "offset":dsi.offset,
                                "size":dsi.size,
                                "status":0,
                                "checksum":dsi.checksum
                            }
                            self.__log.info(event)
                            client_queue.put_nowait(event)
                        else:
                            event = {
                                "arrival_time":start_time,
                                "client_id":client_id,
                                "task_id":task_id,
                                "key":_key,
                                "path": path,
                                "operation_type":operation_type,
                                "status":-1,
                                "msg":"{} not found".format(key)
                            }
                            self.__log.error(event)
                    else:
                        pass
        except Exception as e:
            print(e)
        finally:
            self.__done_event.set()
            # self.__done_event.set()

    def shutdown(self):
        try:
            self.__log.debug({
                "event":"SHUTDOWN",
                "id":self.__queue_id,
                "msg":"Cleaning all resources",
            })
            self.__is_running = False
            self.__daemon_thread.join()
            self.__q.unlink()
            self.__q.close()
            self.__shared_memory.close()
            self.__shared_memory.unlink()
        except Exception as e:
            print("Error closing or unlinking the queue / shared memory: {}".format(self.__queue_id))

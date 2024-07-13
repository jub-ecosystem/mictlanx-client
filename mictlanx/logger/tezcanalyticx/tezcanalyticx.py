import os
import time as T
from mictlanx.logger.log import JsonFormatter,Log
import logging
from typing import Dict,List
import requests as R
import json as J
from threading import Thread
from queue import Queue,Empty
import humanfriendly as HF
from option import Result,Ok,Err

TEZCANALYTICX_URL = os.environ.get("TEZCANALYTICX_URL","localhost:45000")

class TezcanaliticXHttpHandlerDaemon(Thread):
    def __init__(self,
        q:Queue,
        url:str,
        buffer_size:int = 10,
        heartbeat:str = "5s",
        flush_timeout:str="30s",
        name: str="tezcanalyticx",
        daemon:bool = True
    ) -> None:
        Thread.__init__(self,name=name,daemon=daemon)
        self.url = url
        self.last_flush_at = T.time()
        self.is_running = True
        self.q=q
        self.buffer:List[Dict[str,str]] = []
        self.max_buffer = buffer_size
        self.flush_timeout = HF.parse_timespan(flush_timeout)
        self.heartbeat = HF.parse_timespan(heartbeat)

    def flush(self)->Result[int,Exception]:
        if len(self.buffer)>0:
            json_data = J.dumps(self.buffer)
            try:
                response = R.post(self.url, json=json_data, headers={"Content-Type":"application/json"} )
                response.raise_for_status()
                return len(self.buffer)
            except Exception as e:
                return Err(e)
            finally:
                self.buffer=[]
        else:
            return Ok(0)
            
    def can_flush(self):
        return (T.time() - self.last_flush_at) >= self.flush_timeout

    def run(self) -> None:
        while self.is_running:
            try:
                event = self.q.get_nowait()
                if event == -1:
                    _ = self.flush()
                else:
                    self.buffer.append(event)
            except Empty as e:
                if self.can_flush():
                    self.last_flush_at = T.time()
                    _  = self.flush()
            finally:
                T.sleep(self.heartbeat)



class TezcanalyticXParams(object):
    def __init__(self,
        flush_timeout:str="10s",
        buffer_size:int = 10,
        path:str="/api/v4/events",
        port:int = 45000,
        hostname:str ="localhost",
        protocol:str ="http", level: int = 0
    ):
        self.flush_timeout = flush_timeout
        self.buffer_size = buffer_size
        self.protocol = protocol
        self.hostname = hostname
        self.port = port
        self.path = path
        self.level = level

class TezcanalyticXHttpHandler(logging.Handler):
    def __init__(self,
        flush_timeout:str="30sec",
        heartbeat:str = "5sec",
        buffer_size:int = 10,
        path:str="/api/v4/events",
        port:int = 45000,
        hostname:str ="localhost",
        protocol:str ="http", level: int = 0
    ):
        super().__init__(level)
        self.protocol = protocol
        self.hostname = hostname
        self.port = port
        self.path = path
        self.q = Queue(maxsize=buffer_size)
        self.url= "{}://{}{}".format(self.protocol,self.hostname,self.path) if port<=0 else "{}://{}:{}{}".format(self.protocol,self.hostname,self.port,self.path)
        self.buffer_size = buffer_size
        self.emit_counter = 0
        self.daemon = TezcanaliticXHttpHandlerDaemon(

            url=self.url,
            q = self.q,
            buffer_size=self.buffer_size,
            flush_timeout=flush_timeout,
            heartbeat= heartbeat,
            name="tezcanalyticx-daemon",
            daemon=True
        )
        self.daemon.start()
        self.setFormatter(JsonFormatter())
        # self.failed_requests_buffer:List[logging.LogRecord ] = []
    def format(self,record:logging.LogRecord):
        log_data = {
            # "message":record.getMessage(),
            "level":record.levelname,
            "name":record.name,
            "time":self.formatter.formatTime(record=record, datefmt="%Y-%m-%d %H:%M:%S"),
            "timestamp":int(T.time())
        }

        if isinstance(record.msg, dict):
            log_data.update(record.msg)  # Add the dictionary data to the log
        else:
            log_data['message'] = record.getMessage()
        return log_data
        
    def emit(self, record: logging.LogRecord):
        self.emit_counter+=1
        _r = self.format(record=record)
        self.q.put(_r)
        if self.emit_counter % self.buffer_size == 0:
            self.q.put(-1)


if __name__ == "__main__":
    tezcanalyticx_handler = TezcanalyticXHttpHandler()
    L = Log()
    L.addHandler(tezcanalyticx_handler)
    N = 100
    for i in range(N):
        L.debug({
            "counter":i,
            "popularity":0,
            "response_time":0
        })
        T.sleep(.5)
    T.sleep(1000)
    
import os
import sys
import logging
import json
import threading
from typing import Any
from option import Some,NONE,Option
# from pathlib import Path

class DumbLogger(object):
    def debug(self,**kargs):
        return
    def info(self,**kargs):
        return
    def error(self,**kargs):
        return


class JsonFormatter(logging.Formatter):
    def format(self, record):
        thread_id = threading.current_thread().getName()
        log_data = {
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            # 'message': record.getMessage(),
            'logger_name': record.name,
            "thread_name":thread_id
        }
        if isinstance(record.msg, dict):
            log_data.update(record.msg)  # Add the dictionary data to the log
        else:
            log_data['message'] = record.getMessage()

        return json.dumps(log_data)



class Log(logging.Logger):
    def __init__(self,
                 formatter:logging.Formatter=JsonFormatter(),
                 name:str ="mictlanx-client-0",
                 level:int = logging.DEBUG,
                 path:str = "/mictlanx/client",
                 disabled:bool = False,
                 console_handler_filter =lambda record: record.levelno == logging.DEBUG,
                 file_handler_filter =lambda record: record.levelno == logging.INFO,
                 console_handler_level:int = logging.DEBUG,
                 file_handler_level:int = logging.INFO,
                #  format:str = '%(asctime)s %(levelname)s %(threadName)s %(message)s',
                #  extension:str = "log",
                 error_log:bool = False,
                 filename:Option[str] = NONE,
                 output_path:Option[str] =NONE,
                 error_output_path:Option[str] = NONE,
                 create_folder:bool= True,
                 to_file:bool = True
                #  "/mictlanx/client/mictlanx-client-0.error", 
                 ):
        super().__init__(name,level)
        if (not os.path.exists(path) and create_folder) :
            os.makedirs(path)
            
        if not (disabled):
            consolehanlder =logging.StreamHandler(sys.stdout)
            consolehanlder.setFormatter(formatter)
            consolehanlder.setLevel(console_handler_level)
            consolehanlder.addFilter(console_handler_filter)
            self.addHandler(consolehanlder)
            if to_file:
                filehandler = logging.FileHandler(filename= output_path.unwrap_or("{}/{}.log".format(path,filename.unwrap_or(name))))
                filehandler.setFormatter(formatter)
                filehandler.setLevel(file_handler_level)
                filehandler.addFilter(file_handler_filter)
                self.addHandler(filehandler)
            # 
            if error_log:
                errorFilehandler = logging.FileHandler(filename=error_output_path.unwrap_or("{}/{}.error".format(path,filename.unwrap_or(name))))
                errorFilehandler.setFormatter(formatter)
                errorFilehandler.setLevel(logging.ERROR)
                errorFilehandler.addFilter(lambda record: record.levelno == logging.ERROR)
                self.addHandler(errorFilehandler)
        
        # name                   = kwargs.get("name","deafult")
        # level                  = kwargs.get("level",logging.NOTSET)
        # path                   = kwargs.get("path","/mictlanx/client/")
        # filename               = kwargs.get("filename",name)
        # disabled               = kwargs.get("disabled",False)
        # console_handler_filter = kwargs.get("console_handler_filter", lambda record: record.levelno == logging.DEBUG)
        # file_handler_filter    = kwargs.get("file_handler_filter", lambda record: record.levelno == logging.INFO)
        # console_handler_level  = kwargs.get("console_handler_level",logging.NOTSET)
        # file_hanlder_level     = kwargs.get("file_handler_level",logging.INFO)
        # format_str             = kwargs.get("format_str",'%(asctime)s %(levelname)s %(threadName)s %(message)s')
        
        # formatter              = kwargs.get("formatter",JsonFormatter()
        #                                     # logging.Formatter(format_str,"%Y-%m-%d %H:%M:%S")
        #                                     )
        # extension              = kwargs.get("extesion","log")
        # output_path            = kwargs.get("output_path","{}/{}.{}".format(path,filename,extension))
        # error_output_path      = kwargs.get("error_output_path", "{}/{}-error.{}".format(path,filename,extension))
        # self.setLevel(logging.DEBUG)

# l = Log(name="test",path="/log",level=logging.DEBUG)
# l.debug("AAA")


# class Logger(object):
#     def __init__(self,**kwargs):
#         name                   = kwargs.get("name","default")
#         LOG_PATH               = kwargs.get("LOG_PATH")
#         LOG_FILENAME           = kwargs.get("LOG_FILENAME")
#         add_error_log          = kwargs.get("add_error_log",True)
#         console_handler_filter = kwargs.get("console_handler_filter", lambda record: record.levelno == logging.DEBUG)
#         file_handler_filter    = kwargs.get("file_handler_filter", lambda record: record.levelno == logging.INFO)
#         console_handler_level  = kwargs.get("console_handler_level",logging.DEBUG)
#         file_hanlder_level     = kwargs.get("file_handler_level",logging.INFO)
#         # self.logger            = 
#         # 
#         FORMAT      = '%(asctime)s %(msecs)s %(levelname)s %(threadName)s %(message)s'
#         formatter   = logging.Formatter(FORMAT,"%Y-%m-%d,%H:%M:%S")
#         # 
#         filename      = "{}/{}.log".format(LOG_PATH,LOG_FILENAME)
#         errorFilename = "{}/{}-error.log".format(LOG_PATH,LOG_FILENAME)
#         # 
#         self.logger   = logging.getLogger(name)
#         # ___________________________________
#         consolehanlder =logging.StreamHandler(sys.stdout)
#         consolehanlder.setFormatter(formatter)
#         consolehanlder.setLevel(console_handler_level)
#         consolehanlder.addFilter(console_handler_filter)
#         # 
#         filehandler = logging.FileHandler(filename= filename)
#         filehandler.setFormatter(formatter)
#         filehandler.setLevel(file_hanlder_level)
#         filehandler.addFilter(file_handler_filter)
#         # 
#         if(add_error_log):
#             errorFilehandler = logging.FileHandler(filename=errorFilename)
#             errorFilehandler.setFormatter(formatter)
#             errorFilehandler.setLevel(logging.ERROR)
#             errorFilehandler.addFilter(lambda record: record.levelno == logging.ERROR)
#             self.logger.addHandler(errorFilehandler)
#         # 
#         self.logger.addHandler(filehandler)
#         self.logger.addHandler(consolehanlder)
#         # 
#         self.logger.setLevel(logging.DEBUG)
#         # 
#         return self.logger

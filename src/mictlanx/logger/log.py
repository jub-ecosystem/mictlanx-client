import sys
import logging

class DumbLogger(object):
    def debug(self,**kargs):
        return
    def info(self,**kargs):
        return
    def error(self,**kargs):
        return

import logging 
import sys 
class Log(logging.Logger):
    def __init__(self,**kwargs):
        name                   = kwargs.get("name","deafult")
        level                  = kwargs.get("level",logging.NOTSET)
        path                   = kwargs.get("path","/log")
        filename               = kwargs.get("filename",name)
        disabled               = kwargs.get("disabled",False)
        console_handler_filter = kwargs.get("console_handler_filter", lambda record: record.levelno == logging.DEBUG)
        file_handler_filter    = kwargs.get("file_handler_filter", lambda record: record.levelno == logging.INFO)
        console_handler_level  = kwargs.get("console_handler_level",logging.NOTSET)
        file_hanlder_level     = kwargs.get("file_handler_level",logging.INFO)
        format_str             = kwargs.get("format_str",'%(asctime)s %(msecs)s %(levelname)s %(threadName)s %(funcName)s %(message)s')
        formatter              = kwargs.get("formatter",logging.Formatter(format_str,"%Y-%m-%d,%H:%M:%S"))
        extension              = kwargs.get("extesion","log")
        output_path            = kwargs.get("output_path","{}/{}.{}".format(path,filename,extension))
        error_output_path      = kwargs.get("error_output_path", "{}/{}-error.{}".format(path,filename,extension))
        # print("LOG_OUTPUT_PATH",output_path)

        super().__init__(name,level)
        if not (disabled):
            consolehanlder =logging.StreamHandler(sys.stdout)
            consolehanlder.setFormatter(formatter)
            consolehanlder.setLevel(console_handler_level)
            consolehanlder.addFilter(console_handler_filter)
            # 
            filehandler = logging.FileHandler(filename= output_path)
            filehandler.setFormatter(formatter)
            filehandler.setLevel(file_hanlder_level)
            filehandler.addFilter(file_handler_filter)
            # if(add_error_log):
            errorFilehandler = logging.FileHandler(filename=error_output_path)
            errorFilehandler.setFormatter(formatter)
            errorFilehandler.setLevel(logging.ERROR)
            errorFilehandler.addFilter(lambda record: record.levelno == logging.ERROR)
            self.addHandler(errorFilehandler)
            self.addHandler(filehandler)
            self.addHandler(consolehanlder)
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

import sys
import logging

class DumbLogger(object):
    def debug(self,**kargs):
        return
    def info(self,**kargs):
        return
    def error(self,**kargs):
        return

def create_logger(**kwargs):
    name                   = kwargs.get("name","default")
    LOG_PATH               = kwargs.get("LOG_PATH")
    LOG_FILENAME           = kwargs.get("LOG_FILENAME")
    add_error_log          = kwargs.get("add_error_log",True)
    console_handler_filter = kwargs.get("console_handler_filter", lambda record: record.levelno == logging.DEBUG)
    file_handler_filter    = kwargs.get("file_handler_filter", lambda record: record.levelno == logging.INFO)
    # 
    FORMAT      = '%(asctime)s,%(msecs)s,%(levelname)s,%(threadName)s,%(message)s'
    formatter   = logging.Formatter(FORMAT,"%Y-%m-%d,%H:%M:%S")
    # 
    filename      = "{}/{}.log".format(LOG_PATH,LOG_FILENAME)
    errorFilename = "{}/{}-error.log".format(LOG_PATH,LOG_FILENAME)
    # 
    logger      = logging.getLogger(name)
    # ___________________________________
    consolehanlder =logging.StreamHandler(sys.stdout)
    consolehanlder.setFormatter(formatter)
    consolehanlder.setLevel(logging.DEBUG)
    consolehanlder.addFilter(console_handler_filter)
    # 
    filehandler = logging.FileHandler(filename= filename)
    filehandler.setFormatter(formatter)
    filehandler.setLevel(logging.INFO)
    filehandler.addFilter(file_handler_filter)
    # 
    if(add_error_log):
        errorFilehandler = logging.FileHandler(filename=errorFilename)
        errorFilehandler.setFormatter(formatter)
        errorFilehandler.setLevel(logging.ERROR)
        errorFilehandler.addFilter(lambda record: record.levelno == logging.ERROR)
        logger.addHandler(errorFilehandler)
    # 
    logger.addHandler(filehandler)
    logger.addHandler(consolehanlder)
    # 
    logger.setLevel(logging.DEBUG)
    # 
    return logger

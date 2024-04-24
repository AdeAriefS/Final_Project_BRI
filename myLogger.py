import logging
import time
 
import logging.handlers # import RotatingFileHandler
 
loggingPath = './logs/'
 
def setup_logger(logger,filename):
    log_handler = logging.handlers.TimedRotatingFileHandler(loggingPath+filename+'.log',  when='D',backupCount=5)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
 
def getFilenameLoggingHandlers(handler):
    strTest = ''
    strTest = str(rotatingTimeHandlers)
    strArr = str.split(' ')
    print(strArr)
    filePathName = strArr[1]
    fileNameArr = filePathName.split('/')
    fileName = fileNameArr[len(fileNameArr)-1]
    return fileName
 
def logging_setup(filename):
    logger = logging.getLogger(filename)
    log_handler = logging.handlers.TimedRotatingFileHandler(loggingPath+filename+'.log',  when='D',backupCount=5)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
 
def logging_info(filename,*args):
    logger = logging.getLogger(filename)
    if len(logger.handlers) == 0:
        logging_setup(filename)
 
    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)
     
    message = ""
    for var in args:
        message += str(var)
     
    logger.info(message)
 
def logging_debug(filename,*args):
    logger = logging.getLogger(filename)
    if len(logger.handlers) == 0:
        logging_setup(filename)
 
    logger = logging.getLogger(filename)
    logger.setLevel(logging.DEBUG)
     
    message = ""
    for var in args:
        message += str(var)
     
    logger.debug(message)
     
def logging_error(filename,*args):
    logger = logging.getLogger(filename)
    if len(logger.handlers) == 0:
        logging_setup(filename)
 
    logger = logging.getLogger(filename)
    logger.setLevel(logging.ERROR)
     
    message = ""
    for var in args:
        message += str(var)
     
    logger.error(message)

import logging
import multiprocessing
import os
import traceback
from logConfig import logConfig
from configLoader import cfg, configPath

def _locked_emit(emit, lock):
    def wrapper(record):
        with lock:
            emit(record)

    return wrapper

os.makedirs(f"{configPath}/logs", exist_ok=True)
class Logger:
    loggers = {}
    fileHandlers = {}
    logLock = multiprocessing.Lock()
    formatter = logging.Formatter("%(asctime)s - %(levelname)-9s - %(message)s")
    fileHandler = logging.FileHandler(f"{configPath}/logs/default.log")

    @classmethod
    def setProcessName(self, name):
        if name != "":
            if len(name) > 10:
                name = name[:10]
            else:
                while len(name) < 10:
                    name += " "
            multiprocessing.current_process().name = name
     
    def __init__(self, name = '', loggers= ['default']): 
        self.loggers = []
        self.display = False
        self.name = name
        for logger in loggers:
            if logger == 'None':
                self.logDebug = self._emptyLog
                self.logInfo = self._emptyLog
                self.logWarn = self._emptyLog
                self.logCritical = self._emptyLog  
                break
            else:
                if logger not in Logger.loggers:
                    l = logging.getLogger(logger)
                    l.setLevel(logging.INFO)
                    Logger.loggers[logger] = l
                    fileHandler = logging.FileHandler(f"{configPath}/logs/{logger.replace('.', '').replace('/', '')[:10]}.log")
                    fileHandler.setFormatter(Logger.formatter)
                    # fileHandler.emit = _locked_emit(fileHandler.emit, Logger.logLock)
                    l.addHandler(fileHandler)
                else:
                    l = Logger.loggers[logger]
                self.loggers.append(l)
 
    # def logCalls(self, fn, message, display=False, trace=False):
    #     def wrappedFn():
    #         self.logInfo(message, display, trace)
    def _emptyLog(self, data, display=False, trace=False):
        pass

    def logDebug(self, message, display=False, trace=False):
        message = f"{multiprocessing.current_process().name} - {self.name} - {message}"
        for logger in self.loggers:
            logger.debug(message)
        if display or self.display:
            print(message)
        if trace:
            traceback.print_exc()

    def logInfo(self, message, display=False, trace=False):
        message = f"{multiprocessing.current_process().name} - {self.name} - {message}"
        for logger in self.loggers:
            logger.info(message)
        if display or self.display:
            print(message)
        if trace:
            traceback.print_exc()

    def logWarn(self, message, display=True, trace=True):
        message = f"{multiprocessing.current_process().name} - {self.name} - {message}"
        for logger in self.loggers:
            logger.warn(message)
        if display or self.display:
            print(message)
            if trace:
                traceback.print_exc()

    def logCritical(self, message, display=True, trace=True):
        message = f"{multiprocessing.current_process().name} - {self.name} - {message}"
        for logger in self.loggers:
            logger.critical(message)
        if display or self.display:
            print(message)
            if trace:
                traceback.print_exc()



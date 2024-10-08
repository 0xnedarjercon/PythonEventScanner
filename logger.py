import logging
import multiprocessing
import os
import traceback
from logConfig import logConfig


def _locked_emit(emit, lock):
    def wrapper(record):
        with lock:
            emit(record)

    return wrapper


class Logger:
    fileHandlers = {}
    logLock = multiprocessing.Lock()
    formatter = logging.Formatter("%(asctime)s - %(levelname)-9s - %(message)s")

    @classmethod
    def setProcessName(self, name):
        if name != "":
            if len(name) > 10:
                name = name[:10]
            else:
                while len(name) < 10:
                    name += " "
            multiprocessing.current_process().name = name

    def __init__(self, path='', cfg = '', name = ''):
        if path and cfg:
            debugLevel = cfg["DEBUGLEVEL"]
            if debugLevel != "NONE":
                self.name = name
                self.loggers = []
                os.makedirs(path + "logs/", exist_ok=True)
                for logName in cfg["LOGNAMES"]:
                    self.loggers.append(logging.getLogger(logName))
                    self.loggers[-1].setLevel(logConfig[debugLevel]["DEBUGLEVEL"])
                    if logName not in Logger.fileHandlers:
                        fileHandler = logging.FileHandler(f"{path}/logs/{logName}.log")
                        fileHandler.setFormatter(Logger.formatter)
                        fileHandler.emit = _locked_emit(fileHandler.emit, Logger.logLock)
                        Logger.fileHandlers[logName] = fileHandler
                        self.loggers[-1].addHandler(Logger.fileHandlers[logName])
                        print(f'{logName} added fileHandler')

            else:
                self.logDebug = self._emptyLog
                self.logInfo = self._emptyLog
                self.logWarn = self._emptyLog
                self.logCritical = self._emptyLog
        else:
                self.logDebug = self._emptyLog
                self.logInfo = self._emptyLog
                self.logWarn = self._emptyLog
                self.logCritical = self._emptyLog        
        
    def _emptyLog(self, data, display=False, trace=False):
        pass

    def logDebug(self, message, display=False, trace=False):
        message = f"{self.name} - {message}"
        for logger in self.loggers:
            logger.debug(message)
        if display:
            print(message)
        if trace:
            traceback.print_exc()

    def logInfo(self, message, display=False, trace=False):
        message = f"{self.name} - {message}"
        for logger in self.loggers:
            logger.info(message)
        if display:
            print(message)
        if trace:
            traceback.print_exc()

    def logWarn(self, message, display=True, trace=True):
        message = f"{self.name} - {message}"
        for logger in self.loggers:
            logger.warn(message)
        if display:
            print(message)
            if trace:
                traceback.print_exc()

    def logCritical(self, message, display=True, trace=True):
        message = f"{self.name} - {message}"
        for logger in self.loggers:
            logger.critical(message)
        if display:
            print(message)
            if trace:
                traceback.print_exc()



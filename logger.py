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

    def __init__(self, path, cfg):
        debugLevel = cfg["DEBUGLEVEL"]
        if debugLevel != "NONE":
            os.makedirs(path + "logs/", exist_ok=True)
            self.log = logging.getLogger(cfg["LOGNAMES"][0])
            self.log.setLevel(logConfig[debugLevel]["DEBUGLEVEL"])
            for logName in cfg["LOGNAMES"]:
                if logName not in Logger.fileHandlers:
                    fileHandler = logging.FileHandler(f"{path}/logs/{logName}.log")
                    fileHandler.setFormatter(Logger.formatter)
                    fileHandler.emit = _locked_emit(fileHandler.emit, Logger.logLock)
                    Logger.fileHandlers[logName] = fileHandler
                self.log.addHandler(Logger.fileHandlers[logName])

        else:
            self.logDebug = self._emptyLog
            self.logInfo = self._emptyLog
            self.logWarn = self._emptyLog
            self.logCritical = self._emptyLog

    def _emptyLog(self, data, display=False, trace=False):
        pass

    def logDebug(self, message, display=False, trace=False):
        message = f"{multiprocessing.current_process().name} - {message}"
        self.log.debug(message)
        if display:
            print(message)
        if trace:
            traceback.print_exc()

    def logInfo(self, message, display=False, trace=False):
        message = f"{multiprocessing.current_process().name} - {message}"
        self.log.info(message)
        if display:
            print(message)
        if trace:
            traceback.print_exc()

    def logWarn(self, message, display=True, trace=True):
        message = f"{multiprocessing.current_process().name} - {message}"
        self.log.warning(message)
        if display:
            print(message)
            if trace:
                traceback.print_exc()

    def logCritical(self, message, display=True, trace=True):
        message = f"{multiprocessing.current_process().name} - {message}"
        self.log.critical(message)
        if display:
            print(message)
            if trace:
                traceback.print_exc()



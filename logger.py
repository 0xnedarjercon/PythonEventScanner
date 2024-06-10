import logging
import multiprocessing
import os
from configLoader import configPath, folderPath
import traceback
from logConfig import logConfig


def _locked_emit(emit):
    def wrapper(record):
        with log_lock:
            emit(record)

    return wrapper


os.makedirs(configPath + "logs/", exist_ok=True)
log_file = configPath + "logs/" + folderPath + ".log"
log_lock = multiprocessing.Lock()
formatter = logging.Formatter("%(asctime)s - %(levelname)-9s - %(message)s")
baseLogger = logging.getLogger("baseLogger")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
file_handler.emit = _locked_emit(file_handler.emit)


class Logger:
    @classmethod
    def setProcessName(self, name):
        if name != "":
            if len(name) > 10:
                name = name[:10]
            else:
                while len(name) < 10:
                    name += " "
            multiprocessing.current_process().name = name

    def __init__(self, debugLevel="HIGH"):
        if debugLevel is not None and debugLevel != "NONE":
            self.log = baseLogger
            self.log.setLevel(logConfig[debugLevel]["DEBUGLEVEL"])

            self.log.addHandler(file_handler)
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

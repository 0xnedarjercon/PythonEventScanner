import logging
import multiprocessing
import os
from logging.handlers import QueueHandler, QueueListener
from configLoader import configPath, folderPath
import traceback
from logConfig import logConfig

log_queue = multiprocessing.Queue()
os.makedirs(configPath + "logs/", exist_ok=True)
log_file = configPath + "logs/" + folderPath + ".log"
log_lock = multiprocessing.Lock()
listener = None


def startListener():
    global listener
    if listener == None:
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        listener = QueueListener(log_queue, file_handler)
        listener.propagate = False
        listener.start()
        listener = listener


class Logger:

    def __init__(self, name, debugLevel="HIGH"):

        if debugLevel is not None:
            self.log = logging.getLogger(name)
            self.log.setLevel(logConfig[debugLevel]["DEBUGLEVEL"])
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            file_handler.emit = self._locked_emit(file_handler.emit)
            self.log.addHandler(file_handler)
            self.enabled = True
        else:
            self.enabled = False

    def _locked_emit(self, emit):
        def wrapper(record):
            with log_lock:
                emit(record)

        return wrapper

    def logDebug(self, data, display=False, trace=False):
        if self.enabled:
            self.log.debug(data)
        if display:
            print(data)
        if trace:
            traceback.print_exc()

    def logInfo(self, data, display=False, trace=False):
        if self.enabled:
            self.log.info(data)
        if display:
            print(data)
        if trace:
            traceback.print_exc()

    def logWarn(self, data, display=True, trace=True):
        if self.enabled:
            self.log.warning(data)
        if display:
            print(data)
            if trace:
                traceback.print_exc()

    def logCritical(self, data, display=True, trace=True):
        if self.enabled:
            self.log.critical(data)
        if display:
            print(data)
            if trace:
                traceback.print_exc()


def worker_process(name):
    logger = Logger(name)
    while True:
        logger.logInfo(f"Process {name} is running")
        logger.logInfo(f"Process {name} is done")


if __name__ == "__main__":

    # Configure and start the logging listener
    listener = startListener()

    # Create and start processes
    process_names = [f"Process-{i}" for i in range(5)]
    processes = [
        multiprocessing.Process(target=worker_process, args=(name,))
        for name in process_names
    ]
    for p in processes:
        p.start()

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # Stop the listener
    listener.stop()

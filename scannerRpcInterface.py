import multiprocessing
import copy
import time
from contextlib import contextmanager
from logger import Logger


class Synchronous:
    pass


@contextmanager
def tryLock(lock):
    acquired = lock.acquire(block=False)
    try:
        yield acquired
    finally:
        if acquired:
            lock.release()


class ScannerRPCInterface(Logger):
    def __init__(self, settings):
        super().__init__("RPCInterface", settings["DEBUGLEVEL"])
        self.manager = multiprocessing.Manager()
        self._state = self.manager.Value("i", 0)
        self._start = self.manager.Value("i", 0)
        self._end = self.manager.Value("i", 0)
        self.sync = self.manager.Namespace()
        self.sync.request = None
        self.sync.result = None
        self.sync.count = 0
        self._results = self.manager.dict()
        self._fixedScanResults = self.manager.list()

        # Creating reentrant locks for each variable
        self._state_lock = multiprocessing.RLock()
        self._lastBlock_lock = multiprocessing.RLock()
        self._start_lock = multiprocessing.RLock()
        self._end_lock = multiprocessing.RLock()
        self._fixedScanResult_lock = multiprocessing.RLock()
        self._fixedScanResults_lock_condition = multiprocessing.Condition(
            self._fixedScanResult_lock
        )
        self._sync_request_lock = multiprocessing.RLock()
        self._sync_request_lock_condition = multiprocessing.Condition(
            self._sync_request_lock
        )
        self._sync_results_lock = multiprocessing.RLock()
        self._sync_result_lock_condition = multiprocessing.Condition(
            self._sync_results_lock
        )
        self._results_lock = multiprocessing.RLock()
        self._results_lock_condition = multiprocessing.Condition(self._results_lock)

    def syncRequest(self, request, args=(), kwargs={}, count=1):
        # wait for no current jobs
        with self._sync_request_lock:
            while self.sync.request != None:
                with self._sync_request_lock_condition:
                    self.logInfo("sync request busy, waiting")
                    self._sync_request_lock_condition.wait()
                # add the request
            self.sync.request = (request, args, kwargs)
            self.sync.count = count
        self.logInfo(f"sync request set {request}")
        # wait for result
        with self._sync_results_lock:
            while self.sync.result == None:
                with self._sync_result_lock_condition:
                    self._sync_result_lock_condition.wait()

            # cleanup and return result
            with self._sync_request_lock:
                result = copy.deepcopy(self.sync.result)
                self.sync.result = None
                self.sync.request = None
                self.logInfo(f"sync result received")
                return result

    def checkSyncRequest(self, instance):
        # make local copy of the job, skip if not available
        with tryLock(self._sync_request_lock) as acquired:
            if not acquired:
                return
            if self.sync.count == 0:
                return
            request = copy.deepcopy(self.sync.request)
            self.sync.count -= 1

        # set result and notify
        with self._sync_results_lock:
            self.logInfo(f"sync request received processing...")
            result = self.call_function(instance, request[0], request[1], request[2])
            with self._sync_results_lock:
                self.sync.result = result
                with self._sync_result_lock_condition:
                    self._sync_result_lock_condition.notify_all()
                    self.logInfo(f"sync result posted notifying...")

    def call_function(self, instance, request, args, kwargs):
        methods = request.split(".")
        obj = instance
        for i in range(len(methods)):
            obj = getattr(obj, methods[i])
        return obj(*args, **kwargs)

    @property
    def state(self):
        with self._state_lock:
            return self._state.value

    @state.setter
    def state(self, value):
        with self._state_lock:
            self._state.value = value

    @property
    def start(self):
        with self._start_lock:
            return self._start.value

    @start.setter
    def start(self, value):
        with self._start_lock:
            self._start.value = value

    @property
    def end(self):
        with self._end_lock:
            return self._end.value

    @end.setter
    def end(self, value):
        with self._end_lock:
            self._end.value = value

    def setScanRange(self, start, end):
        with self._start_lock:
            self._start.value = start
        with self._end_lock:
            self._end.value = end

    def getScanJob(self, maxSize):
        with self._start_lock:
            with self._end_lock:
                if (self._end.value) != 0 and self._end.value != self._start.value:
                    startBlock = self._start.value
                    endBlock = min([startBlock + maxSize, self._end.value])
                    self._start.value = endBlock
                    self.logInfo(
                        f"distributed job, remaining range: {self._start.value} - {self._end.value}"
                    )
                    return (startBlock, endBlock)
                else:
                    return []

    def addScanResults(self, result):
        with self._fixedScanResult_lock:
            self._fixedScanResults.append(result)
            with self._fixedScanResults_lock_condition:
                self.logInfo(f"scan results added, blocks {result[0]}-{result[2]}")
                self._fixedScanResults_lock_condition.notify_all()

    def readScanResults(self):
        with self._fixedScanResult_lock:
            while len(self._fixedScanResults) == 0:
                with self._fixedScanResults_lock_condition:
                    self._fixedScanResults_lock_condition.wait()
            results = copy.deepcopy(self._fixedScanResults)
            self._fixedScanResults[:] = []
            self.logInfo(f"scan results read and cleared")
            return results

    def getLiveResults(self, blocking=True):
        with self._results_lock:
            if blocking:
                while len(self._results) == 0:
                    with self._results_lock_condition:
                        self._results_lock_condition.wait()
            elif len(self._results) == 0:
                return {}
            results = copy.deepcopy(self._results)
            self._results.clear()
            self.logInfo(f"live scan results read and cleared")
        return results

    def addLiveResults(self, data):
        start = self.start
        data = {key: value for key, value in data.items() if (key) > start}
        if not data:
            return
        latestBlock = list(data.keys())[-1]
        with self._results_lock:
            self._results.update(data)
            self.start = self.end = latestBlock
            with self._results_lock_condition:
                self.logInfo(f"live scan results added from {start} to {latestBlock}")
                self._results_lock_condition.notify_all()

import multiprocessing
import copy
import time
from contextlib import contextmanager


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


class ScannerRPCInterface:
    def __init__(self):
        self.manager = multiprocessing.Manager()
        self._state = self.manager.Value("i", 0)
        self._start = self.manager.Value("i", 0)
        self._end = self.manager.Value("i", 0)
        self.sync = self.manager.Namespace()
        self.sync.request = None
        self.sync.result = None
        self.sync.count = 0
        self._results = self.manager.dict()

        # Creating reentrant locks for each variable
        self._state_lock = multiprocessing.RLock()
        self._lastBlock_lock = multiprocessing.RLock()
        self._start_lock = multiprocessing.RLock()
        self._end_lock = multiprocessing.RLock()
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
                    self._sync_request_lock_condition.wait()
                # add the request
            self.sync.request = (request, args, kwargs)
            self.sync.count = count
        # wait for result
        with self._sync_results_lock:
            while self.sync.result == None:
                with self._sync_result_lock_condition:
                    self._sync_result_lock_condition.wait()

        # cleanup and return result
        with self._sync_results_lock:
            with self._sync_request_lock:
                result = copy.deepcopy(self.sync.result)
                self.sync.result = None
                self.sync.request = None
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
            result = self.call_function(instance, request[0], request[1], request[2])
            with self._sync_results_lock:
                self.sync.result = result
                with self._sync_result_lock_condition:
                    self._sync_result_lock_condition.notify_all()

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
                if (self._end) != 0 and self._end != self._start:
                    startBlock = self._start.value
                    endBlock = min([startBlock + maxSize, self._end.value])
                    self._start.value = endBlock
                    return (startBlock, endBlock)
                else:
                    return []

    def getResults(self, blocking=True):
        with self._results_lock:
            if blocking:
                while len(self._results) == 0:
                    with self._results_lock_condition:
                        self._results_lock_condition.wait()
            elif len(self._results) == 0:
                return {}
            results = copy.deepcopy(self._results)
            self._results.clear()
        return results

    def addResults(self, data):
        start = self.start
        print(type(start))
        data = {key: value for key, value in data.items() if (key) > start}
        if not data:
            return
        latestBlock = list(data.keys())[-1]
        with self._results_lock:
            self._results.update(data)
            self.start = self.end = latestBlock
            with self._results_lock_condition:
                self._results_lock_condition.notify_all()

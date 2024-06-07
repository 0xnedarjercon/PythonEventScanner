import multiprocessing
import copy
from logger import Logger


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
        self._fixedScanRequests = self.manager.list()
        self._results = self.manager.dict()
        self._fixedScanResults = self.manager.list()

        # Creating reentrant locks for each variable
        self._fixedScanRequests_lock = multiprocessing.RLock()
        self._lastBlock_lock = multiprocessing.RLock()
        self._start_lock = multiprocessing.RLock()
        self._end_lock = multiprocessing.RLock()
        self._fixedScanResult_lock = multiprocessing.RLock()
        self._fixedScanResults_lock_condition = multiprocessing.Condition(
            self._fixedScanResult_lock
        )
        self._state_sync_request_lock = multiprocessing.RLock()
        self._state_sync_request_lock_condition = multiprocessing.Condition(
            self._state_sync_request_lock
        )
        self._sync_result_lock = multiprocessing.RLock()
        self._sync_result_lock_condition = multiprocessing.Condition(
            self._sync_result_lock
        )
        self._results_lock = multiprocessing.RLock()
        self._results_lock_condition = multiprocessing.Condition(self._results_lock)

    def syncRequest(self, request, args=(), kwargs={}, count=1):
        # wait for no current jobs
        with self._state_sync_request_lock:
            while self.sync.request != None:
                with self._state_sync_request_lock_condition:
                    self.logInfo("sync request busy, waiting")
                    self._state_sync_request_lock_condition.wait()
                # add the request
            self.sync.request = (request, args, kwargs)
            self.sync.count = count
        self.logInfo(f"sync request set {request}")
        # wait for result
        with self._sync_result_lock:
            while self.sync.result == None:
                with self._sync_result_lock_condition:
                    self._sync_result_lock_condition.wait()

            # cleanup and return result
            with self._state_sync_request_lock:
                result = copy.deepcopy(self.sync.result)
                self.sync.result = None
                self.sync.request = None
                self.logInfo(f"sync result received for {request}")
                self.logDebug(f"result: {result}")
                with self._state_sync_request_lock_condition:
                    self._state_sync_request_lock_condition.notify_all()
                self.logDebug("result lock released")
                return result

    def checkSyncRequest(self, instance, blocking=False):
        # make local copy of the job
        with self._state_sync_request_lock:
            if self.sync.count < 1:
                if not blocking:
                    return
                else:
                    with self._state_sync_request_lock_condition:
                        self.logDebug("waiting for sync request")
                        self._state_sync_request_lock_condition.wait()
                        self.logDebug("finished waiting for sync request")
                        if self.sync.count < 1:
                            return
            request = copy.deepcopy(self.sync.request)
            self.sync.count -= 1

        # set result and notify
        self.logInfo(f"sync request received: {request} processing...")
        result = self.doRequest(instance, request[0], request[1], request[2])
        with self._sync_result_lock:
            self.logDebug("result lock locked")
            self.sync.result = result
            self.logDebug("getting result lock condition")
            with self._sync_result_lock_condition:
                self._sync_result_lock_condition.notify_all()
                self.logInfo(f"sync result posted notifying...")
            self.logDebug("getting result lock condition")
        self.logDebug("result lock released")

    def doRequest(self, instance, request, args, kwargs):
        methods = request.split(".")
        obj = instance
        for i in range(len(methods)):
            obj = getattr(obj, methods[i])
        return obj(*args, **kwargs)

    @property
    def state(self):
        with self._state_sync_request_lock:
            return self._state.value

    @state.setter
    def state(self, value):
        with self._state_sync_request_lock:
            while self.sync.request != None:
                with self._state_sync_request_lock_condition:
                    self._state_sync_request_lock_condition.wait()
            with self._state_sync_request_lock:
                self._state.value = value
                with self._state_sync_request_lock_condition:
                    self._state_sync_request_lock_condition.notify_all()

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

    def addScanRange(self, start, end):
        with self._fixedScanRequests_lock:
            self._fixedScanRequests.insert(0, [start, end])


    def getScanJob(self, maxSize):
        with self._fixedScanRequests_lock:
            while self._fixedScanRequests and self._fixedScanRequests[0][0] == self._fixedScanRequests[0][1]:
                self._fixedScanRequests.pop(0)
            
            if not self._fixedScanRequests:
                return []
            startBlock = self._fixedScanRequests[0][0]
            endBlock = min([startBlock + maxSize, self._fixedScanRequests[0][1]])
            self._fixedScanRequests[0] = (endBlock, self._fixedScanRequests[0][1])
            self.logDebug(
                f"distributed job {(startBlock, endBlock)}, remaining range: {self._fixedScanRequests[0][0]} - {self._fixedScanRequests[0][1]}"
            )
            return (startBlock, endBlock)


    def addScanResults(self, result):
        with self._fixedScanResult_lock:
            self._fixedScanResults.append(result)
            with self._fixedScanResults_lock_condition:
                self.logInfo(f"scan results added, blocks {result[0]}-{result[2]}")
                self._fixedScanResults_lock_condition.notify_all()

    def readScanResults(self, blocking = True):
        with self._fixedScanResult_lock:
            while len(self._fixedScanResults) == 0:
                if blocking:
                    with self._fixedScanResults_lock_condition:
                        self._fixedScanResults_lock_condition.wait()
                else:
                    return []
            results = copy.deepcopy(self._fixedScanResults)
            self.logInfo(f"scan results read and cleared, blocks")
            self._fixedScanResults[:] = []
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
        latestBlock = min(list(data.keys())[-1], start)
        with self._results_lock:
            self._results.update(data)
            self.start = self.end = latestBlock
            with self._results_lock_condition:
                self.logInfo(f"live scan results added from {start} to {latestBlock}")
                self._results_lock_condition.notify_all()

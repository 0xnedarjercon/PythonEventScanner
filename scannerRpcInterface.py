import multiprocessing
import copy


class ScannerRPCInterface:
    def __init__(self, results=None):
        manager = multiprocessing.Manager()
        self._state = manager.Value("i", 0)
        self._start = manager.Value("i", 0)
        self._end = manager.Value("i", 0)
        self._lastBlock = manager.Value("i", 0)
        self._jobs = manager.list()
        if results == None:
            self._results = manager.list()
        else:
            self._results = results

        # Creating reentrant locks for each variable
        self._state_lock = multiprocessing.RLock()
        self._lastBlock_lock = multiprocessing.RLock()
        self._start_lock = multiprocessing.RLock()
        self._end_lock = multiprocessing.RLock()
        self._results_lock = multiprocessing.RLock()

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

    @property
    def lastBlock(self):
        with self._lastBlock_lock:
            return self._lastBlock.value

    @lastBlock.setter
    def lastBlock(self, value):
        with self._lastBlock_lock:
            self._lastBlock.value = value

    def setScanRange(self, start, end):
        with self._start_lock and self._end_lock:
            self._start.value = start
            self._end.value = end

    def getJob(self, maxSize):
        with self._start_lock and self._end_lock:
            if (self._end) != 0 and self._end != self._start:
                startBlock = self._start.value
                endBlock = min([startBlock + maxSize, self._end.value])
                self._start.value = endBlock
                return [startBlock, endBlock]
            else:
                return []

    def getResults(self):
        with self._results_lock:
            if len(self._results) > 0:
                results = copy.deepcopy(self._results)
                self._results[:] = []
                return results
            else:
                return []

    def copyResults(self):
        with self._results_lock:
            if len(self._results) > 0:
                return copy.deepcopy(self._results)
            else:
                return []

    def addResults(self, data):
        with self._results_lock:
            self._results.append(data)

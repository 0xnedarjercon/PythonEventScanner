import multiprocessing
import copy
from logger import Logger


class DummyLock:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def wait(self):
        pass

    def notify_all(self):
        pass


class DummyInt(int):
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        self._value = new_value


class InterProcessResults(Logger):
    def __init__(self, configPath, debugLevel, multiprocess=True):
        super().__init__(configPath, debugLevel)
        if multiprocess:
            manager = multiprocessing.Manager()
            self.results = manager.list()
            self.results_lock_condition = manager.RLock()
            self.results_lock_condition = manager.Condition()
        else:
            self.results = []
            self.results_lock_condition = DummyLock()
            self.results_lock_condition = DummyLock()

    def checkResults(self):
        with self.results_lock_condition:
            self.logInfo("results locked")
            if len(self.results) == 0:
                return []
            results = copy.deepcopy(self.results)
            self.results[:] = []
        return results

    def waitResults(self):
        with self.results_lock_condition:
            self.results_lock_condition.wait_for(self._resultsAvailable)
            results = copy.deepcopy(self.results)
            self.results[:] = []
            self.logInfo("results received")
        return results

    def _resultsAvailable(self):
        return len(self.results) > 0

    def addResults(self, result):
        with self.results_lock_condition:
            self.logInfo("results locked")
            self.results.append(result)
            self.logInfo("results added")
            with self.results_lock_condition:
                self.results_lock_condition.notify_all()
        self.logInfo("results released")


class JobManager(Logger):
    def __init__(self, configPath, interfaceSettings, multiprocess=True):
        super().__init__(configPath, interfaceSettings)
        if multiprocess:
            self.manager = multiprocessing.Manager()
            self._state = self.manager.Value("i", 0)
            self._state_lock = multiprocessing.RLock()
            self.jobs = self.manager.dict()
            self.jobIndex = self.manager.Value("i", 0)
            self.jobs_lock = multiprocessing.RLock()
            self.jobs_lock_condition = multiprocessing.Condition(self.jobs_lock)
        else:
            self._state = DummyInt(0)
            self.jobs = {}
            self.jobIndex = DummyInt(0)
            self.jobs_lock = DummyLock()
            self.jobs_lock_condition = DummyLock()
            self._state_lock = DummyLock()

    @property
    def state(self):
        with self._state_lock:
            return self._state.value

    @state.setter
    def state(self, value):
        with self._state_lock:
            self._state.value = value

    def addJob(self, request, args=(), kwargs={}, count=1, requireHre=False, wait=True):
        with self.jobs_lock:
            self.jobIndex.value += 1
            self.jobs[self.jobIndex.value] = self.createJob(
                request, args, kwargs, count, requireHre
            )
            self.logDebug(f"job added: {request}")
        if wait:
            return self.waitJob(self.jobIndex.value)
        else:
            return self.jobIndex.value

    def waitJob(self, jobId):
        with self.jobs_lock:
            job = self.jobs[jobId]
            while job.result is None:
                with self.jobs_lock_condition:
                    self.jobs_lock_condition.wait()
            result = job.result
            del self.jobs[jobId]
            return result

    def createJob(self, request, args, kwargs, count, requireHre):
        job = self.manager.Namespace()
        job.request = request
        job.args = args
        job.kwargs = kwargs
        job.count = count
        job.requireHre = requireHre
        job.result = None
        return job

    def checkJob(self, instance):
        job = None
        with self.jobs_lock:
            for jobId, _job in self.jobs.items():
                if (
                    (instance.isHH or not _job.requireHre)
                    and jobId not in instance.completedJobs
                    and _job.count > 0
                ):
                    _job.count -= 1
                    job = _job
                    break
        if job != None:
            self.logDebug(f"doing job {jobId} {job.request}")
            self.doRequest(instance, job)
            instance.completedJobs.append(jobId)

    def doRequest(self, instance, job):
        methods = job.request.split(".")
        obj = instance
        for i in range(len(methods)):
            obj = getattr(obj, methods[i])
        result = obj(*job.args, **job.kwargs)
        with self.jobs_lock:
            job.result = result
        with self.jobs_lock_condition:
            self.jobs_lock_condition.notify_all()
            self.logDebug(f"job completed, notfying...")


class FixedScan(InterProcessResults):
    def __init__(self, configPath, interfaceSettings, multiprocess=True):
        manager = multiprocessing.Manager()
        super().__init__(configPath, interfaceSettings, multiprocess)
        if multiprocess:
            self.remaining = manager.list()
            self.remaining_lock = multiprocessing.RLock()
        else:
            self.remaining = []
            self.remaining_lock = DummyLock()

    def addScanRange(self, start, end):
        with self.remaining_lock:
            self.remaining.insert(0, [start, end])

    def getScanJob(self, maxSize):
        with self.remaining_lock:
            if not self.remaining:
                return []
            startBlock = self.remaining[0][0]
            endBlock = min(startBlock + maxSize, self.remaining[0][1])
            self.remaining[0] = (endBlock + 1, self.remaining[0][1])
            if self.remaining[0][0] >= self.remaining[0][1]:
                self.remaining.pop(0)
            return (startBlock, endBlock)


class LiveScan(InterProcessResults):
    def __init__(self, configPath, interfaceSettings, multiprocess=True):
        manager = multiprocessing.Manager()
        super().__init__(configPath, interfaceSettings, multiprocess)
        if multiprocess:
            self._last = manager.Value("i", 0)
            self.last_lock = manager.RLock()
        else:
            self._last = DummyInt(0)
            self.last_lock = DummyLock()

    @property
    def last(self):
        with self.last_lock:
            return self._last.value

    @last.setter
    def last(self, value):
        with self.last_lock:
            self._last.value = value

    def updateLast(self, value):
        with self.last_lock:
            self._last.value = max(value, self._last.value)
            print(self._last.value, value)

    def addResults(self, result):
        with self.last_lock:
            last = self._last.value
            latestBlock = list(result[1].keys())[-1]
            if last > latestBlock:
                return
            result[1] = {key: value for key, value in result[1].items() if (key) > last}
            result[2] = latestBlock
            self._last.value = latestBlock
        super().addResults(result)


def initInterfaces(configPath, interfaceSettings, multiprocess):
    IfixedScan = FixedScan(configPath, interfaceSettings, multiprocess)
    IliveScan = LiveScan(configPath, interfaceSettings, multiprocess)
    IJobManager = JobManager(configPath, interfaceSettings, multiprocess)
    return IfixedScan, IliveScan, IJobManager

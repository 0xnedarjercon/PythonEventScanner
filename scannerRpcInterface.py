import multiprocessing
import copy
from logger import Logger
from configLoader import rpcInterfaceSettings
import threading


class InterProcessResults(Logger):
    def __init__(self, debugLevel):
        super().__init__(debugLevel)
        manager = multiprocessing.Manager()
        self.results = manager.list()
        self.results_lock_condition = manager.RLock()
        self.results_lock_condition = manager.Condition()

    def checkResults(self):
        with self.results_lock_condition:
            self.logInfo("results locked")
            if len(self.results) == 0:
                return []
            results = copy.deepcopy(self.data.results)
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
    def __init__(self):
        super().__init__(rpcInterfaceSettings["DEBUGLEVEL"])
        self.manager = multiprocessing.Manager()
        self._state = self.manager.Value("i", 0)
        self._state_lock = multiprocessing.RLock()
        self.jobs = self.manager.dict()
        self.jobIndex = self.manager.Value("i", 0)
        self.jobs_lock = multiprocessing.RLock()
        self.jobs_lock_condition = multiprocessing.Condition(self.jobs_lock)

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
    def __init__(self):
        manager = multiprocessing.Manager()
        super().__init__(rpcInterfaceSettings["DEBUGLEVEL"])
        self.remaining = manager.list()
        self.remaining_lock = multiprocessing.RLock()

    def addScanRange(self, start, end):
        with self.remaining_lock:
            self.remaining.insert(0, [start, end])

    def getScanJob(self, maxSize):
        with self.remaining_lock:
            if not self.remaining:
                return []
            startBlock = self.remaining[0][0]
            endBlock = min(startBlock + maxSize, self.remaining[0][1])
            self.remaining[0] = (endBlock, self.remaining[0][1])
            if self.remaining[0][0] == self.remaining[0][1]:
                self.remaining.pop(0)
            return (startBlock, endBlock)


class LiveScan(InterProcessResults):
    def __init__(self):
        manager = multiprocessing.Manager()
        super().__init__(rpcInterfaceSettings["DEBUGLEVEL"])
        self._last = manager.Value("i", 0)
        self.last_lock = manager.RLock()

    @property
    def last(self):
        with self.last_lock:
            return self._last.value

    @last.setter
    def last(self, value):
        with self.last_lock:
            self._last.value = value

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


IfixedScan = FixedScan()
IliveScan = LiveScan()
IJobManager = JobManager()

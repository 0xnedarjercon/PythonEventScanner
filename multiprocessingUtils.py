
from multiprocessing import Process
import atexit
import copy
import time
from logger import Logger
import sys
import multiprocessing.managers
from utils import decodeEvents
from utils import blocks, getLastBlock, getW3
import asyncio
from constants import c
from math import sqrt
 
class Job():
    def __init__(self, jobManager, method, args = (), kwargs = {}, target = 0xFF, result =None):
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.target = target
        self.result = result
        self.jobManager = jobManager
class Worker(Process, Logger):
    def __init__(self, job_manager, apiUrl, id, results, lognames,manager, pollPeriod,numPollers, hardhat = False):
        super().__init__()
        self.job_manager = job_manager
        self.apiUrl = apiUrl
        self.w3, self.websocket = getW3(apiUrl)
        self.apiUrl = apiUrl
        self.id = id
        self.results = results
        self.running = True
        self.live = False
        self.hardhat = hardhat
        self.manager = manager
        self.lastBlock = 0
        self.pollIndex = sqrt(self.id)
        self.numPollers = numPollers
        self.pollPeriod = pollPeriod
        self.lastPoll = 0
        atexit.register(self.stop)
        Logger.__init__(self, apiUrl[8:], lognames)
        
    async def asyncRun(self):
        while self.running:
            if not self.runCyclic(wait = False):
                time.sleep(0.1)
    # runs continuously, checking for new jobs in the job manager
    def run(self):
        Logger.setProcessName(self.apiUrl[8:])
        self.logInfo(f'initialised {self.apiUrl} as {self.id}')
        if self.websocket:
            asyncio.run(self.asyncRun())
        while self.running:
            if not self.runCyclic(wait = False):
                time.sleep(0.1)
    

    # checks jobManager for any jobs for this worker and performs them
    async def runCyclic(self, wait = True):
            job = self.job_manager.getJob(self.id, wait = wait)
            if job:
                self.logInfo(f'got job {job[0]} {blocks(job)}')
                result = self.doJob(job)
                self.job_manager.addResult(job, result)
                return True
            else:
                return False

    #goes into livescanning mode, constantly polls for new events and adds them to the shared result           
    def runLive(self, job):
        self.logInfo('running live job')
        if not self.websocket:
            filter = copy.deepcopy(job[1][0])
            lastBlock = filter['fromBlock']
            # checkRange = job[1][1]
            # checkRange = 20
            if 'callback' in job[2]:
                callbackParams = job[2]['callback']
            else:
                callbackParams = None
            while self.live:
                try:
                    filter['fromBlock'] = lastBlock
                    filter['toBlock'] = 'latest'
                    currentTime = time.time()
                    self.nextPoll = currentTime - (currentTime % self.pollPeriod) + (self.pollIndex * self.pollPeriod)
                    results = self.w3.eth.get_logs(filter)
                    if len(results)>0:
                        lastBlock = max(getLastBlock(results), self.lastBlock)
                        if callbackParams != None:
                            results = self.doCallback(results, callbackParams)
                            self.logInfo(f'live job completed {blocks(filter)}')
                        self.results.append(['get_logs_live', (filter,), {}, self.id, results])
                except Exception as e:
                    self.logWarn(f'error in livescan {e}')
                    time.sleep(5)
                while time.time()< self.nextPoll:
                    self.runCyclic(wait = False)
                    time.sleep(0.02)
        else:
            filter = self.w3.eth.filter(filter)
            newEvents = self.filterParams.get_new_entries()
            if len(newEvents) > 0:
                self.handleEvents(newEvents)
            time.sleep(self.pollInterval)
            
    def doCallback(self, results, callbackParams):
        callback = callbackParams[0]
        callbackKwargs = callbackParams[1]
        if 'w3' in callbackKwargs:
            callbackKwargs['w3'] = self.w3
        if 'codec' in callbackKwargs:
            callbackKwargs['codec'] = self.w3.codec
        results = callback(results, **callbackKwargs)
        return results
    #performs a requested job
    def doJob(self, job):
        method, args, kwargs, _, _ = job
        if method == 'stop':
            self.running = False
            return
        elif method == 'live':
            self.logInfo(f'starting live {self.apiUrl}')
            if not self.live:
                self.live = True
                self.runLive(job)
        elif method == 'stopLive':
            self.live = False
        else:
            attr = self.getW3Attr(method)
            if callable(attr):
                try:
                    callback = None
                    if 'callback' in kwargs:
                        callback, callbackKwargs = kwargs.pop('callback')
                        if 'w3' in callbackKwargs:
                            callbackKwargs['w3'] = self.w3
                        if 'codec' in callbackKwargs:
                            callbackKwargs['codec'] = self.w3.codec
                    self.logInfo(f'{method} job started')
                    res=  attr(*args, **kwargs)
                    self.logInfo(f'{method} job finished')
                    if callback:
                        res = callback(res, **callbackKwargs)
                        self.logInfo(f'{method} callback done')
                    return res
                except Exception as e:
                    return e

    #tries to get an attribute from web3, then tries web3.eth
    def getW3Attr(self, method):
        try:
            attr = getattr(self.w3, method)
        except AttributeError as e:
            try:
                attr = getattr(self.w3.eth, method)
            except AttributeError as e:
                attr = getattr(self.w3.eth.account, method)
                
        return attr

    def stop(self):
        self.running = False
     
class SharedResult(Logger):
    def __init__(self, manager,name= 'sr'):
        super().__init__(name)
        self.manager = manager
        self.lock = manager.RLock()
        self.list = manager.list()
    
    def append(self, val):
        with self.lock:
            self.list.append(val)
            
    def addResults(self, val):
        with self.lock:
            self.list+=val

    def clear(self):
        with self.lock():
            self.list.clear()
    
    def get(self, consume = True):
        with self.lock:
            if len(self.list)>0:
                if consume:
                    val = copy.deepcopy(self.list)
                    while len(self.list)> 0:
                        self.list.pop(0)
                    self.logInfo(f'result consumed {[blocks(x) for x in val]})')
                else:
                    val = copy.deepcopy(self.list)
                return val
            else:
                return []
            
            
        
class JobManager(Logger):
    def __init__(self, manager, configPath, web3Settings,name = 'jm'):
        super().__init__(name)
        self.manager = manager
        self.jobs = self.manager.list()
        self.completed = self.manager.list()
        self.lock = self.manager.RLock()


    #appends a new job into the back of the queue
    def addJob(self, method, *args, wait= True,target = None, **kwargs):
            with self.lock:
                job_item = self.manager.list([ method, args, kwargs, target, None])
                self.jobs.append(job_item)
            if wait:
                self.logInfo(f'waiting result {method} {target}')
                return self.waitJob(job_item)
            else:
                return job_item
            
    def waitJob(self, job):
        while True:
            with self.lock:
                if job[-1] != None:
                    return job[-1]
            time.sleep(0.1)
        
        
    def addJobs(self, jobs):
        with self.lock:
            for job in jobs:
                job_item = self.manager.list(job)
                self.jobs.append(job_item)
            return self.jobs[-len(jobs):]
    #inserts a new job into the specified index of the queue
    def insertJob(self, index, method, *args, target = None, **kwargs):
            self.logInfo(f'job added {method}')
            with self.lock:
                job_item = self.manager.list([ method, args, kwargs, target, None])
                self.jobs.insert(index, job_item)
                return job_item  
            
    #inserts a list of jobs into the front of the queue
    def insertJobs(self, jobs):
        with self.lock:
            for job in jobs:
                job_item = self.manager.list(job)
                self.jobs.insert(0, job_item)
            return self.jobs[0:len(jobs)]
    #gets the first available job and removes it from the pending jobs
    def getJob(self, target, wait=True):
        with self.lock:
            for jobIndex, job in enumerate(self.jobs):
                if job[-1] is None and (job[-2]==None or job[-2] & target):
                    self.logInfo(f'job taken {blocks(self.jobs[jobIndex])}')
                    return self.jobs.pop(jobIndex)                   
        while wait:
            time.sleep(c.WAIT)
            with self.lock:
                for jobIndex, job in enumerate(self.jobs):
                    if job[-1] is None and (job[-2]==None or job[-2] &target):
                        return self.jobs.pop(jobIndex)
    #removes all jobs matching the methods and targets specified
    def popAllJobs(self, methods, targets):
        removed = [] 
        with self.lock:
            jobsLength = len(self.jobs)
            for i in range(jobsLength):
                j = jobsLength-i-1
                if self.jobs[j][c.TARGET]&targets and self.jobs[j][c.METHOD] in methods and self.jobs[j]:
                    removed.insert(0, self.jobs.pop(j))
        return removed
    #gets all completed jobs matching the proveded methods and targets
    def getCompletedJobs(self, methods, targets):
        completed = []
        with self.lock:
            for i in range (self.completedJobs):
                job = self.completed[len(self.getCompletedJobs)-i]
                if job[c.METHOD] in methods and targets &job[c.TARGET]:
                    self.logInfo(f'job manager completed jobs consumed')
                    completed.append(self.completed.pop(len(self.getCompletedJobs)-i))
                    
    #adds the result to a job, must be used to ensure it is locked          
    def addResult(self, job, result):
        with self.lock:
            job[-1] = result
            self.logInfo(f'job result added to job manager for {blocks(job)} {type(job[-1])}')
            
    #checks for any completed jobs, removes them inplace and returns the completed ones
    def checkJobs(self, jobs):
        completedJobs = []
        with self.lock:
            i = len(jobs) - 1
            while i >= 0:
                if jobs[i][-1] is not None:
                    self.logInfo(f'comp job found {blocks(jobs[i])}')
                    completedJobs.insert(0,jobs.pop(i))
                    
                i -= 1
        return completedJobs
                  
                    
class ContinuousWrapper:
    def __init__(self, cyclicFn, results=None, *args, **kwargs):
        self.cyclicFn = cyclicFn
        self.running = True
        self.args = args
        self.kwargs = kwargs
        self.results = results
        
    def cyclic(self, *args, **kwargs):
        self.cyclicFn(*args, **kwargs)
        
    def cond(self):
        return False
    
    def continuous(self, stopCondition = None):
        self.running = True
        if stopCondition == None:
            stopCondition = self.cond
        while not stopCondition() and self.running:
            self.cyclicFn(*self.args, results = self.results , **self.kwargs)
            #sleep to allow other threads to take control
            time.sleep(0.001)
            
    def stop(self):
        self.running = False
        
# class PersistentProcessWrapper:
#     def __init__(self, wrapped_class, *args, **kwargs):
#         # Set up a pipe for communication between processes
#         self.parent_conn, self.child_conn = multiprocessing.Pipe()
#         # Store the class to be wrapped and its arguments
#         self.wrapped_class = wrapped_class
#         self.args = args
#         self.kwargs = kwargs
#         # Create and start the worker process
#         self.process = multiprocessing.Process(target=self._worker)
#         self.process.start()
#         # Event loop executor for async method calls
#         self.loop = asyncio.get_event_loop()

#     def _worker(self):
#         # Instantiate the wrapped class in the new process
#         instance = self.wrapped_class(*self.args, **self.kwargs)
#         while True:
#             # Wait for a method call or terminate signal from the parent process
#             method_name, method_args, method_kwargs = self.child_conn.recv()
#             if method_name == '__terminate__':
#                 break
#             # Execute the method and send the result back to the parent process
#             method = getattr(instance, method_name)
#             result = method(*method_args, **method_kwargs)
#             self.child_conn.send(result)

#     async def _async_method(self, method_name, *args, **kwargs):
#         # Send method call asynchronously and await the result
#         await self.loop.run_in_executor(
#             None, self.parent_conn.send, (method_name, args, kwargs)
#         )
#         # Wait asynchronously for the result from the child process
#         return await self.loop.run_in_executor(
#             None, self.parent_conn.recv
#         )

#     def __getattr__(self, method_name):
#         # Return an async function that calls the method in the child process
#         async def async_method(*args, **kwargs):
#             return await self._async_method(method_name, *args, **kwargs)
#         return async_method

#     async def close(self):
#         # Send a termination signal to the child process asynchronously
#         await self._async_method('__terminate__')
#         # Wait for the process to terminate
#         await self.loop.run_in_executor(None, self.process.join)

#     def __del__(self):
#         # Ensure the process is cleaned up when the object is destroyed
#         asyncio.run(self.close())
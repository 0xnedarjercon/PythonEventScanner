from web3 import Web3
from multiprocessing import Queue, Process, Manager, Condition, RLock, Event
from multiprocessing import Manager, Process, Condition, Lock
from rpc import RPC
import atexit
import copy
import time
from logger import Logger
import sys
from configLoader import loadConfig
from hardhat import runHardhat
import multiprocessing.managers
from utils import blocks, toNative
import asyncio
from constants import c


def getW3(cfg):
    if type(cfg) == dict:
        apiURL = cfg["APIURL"]
    else:
        apiURL = cfg
    if apiURL[0:3] == "wss":
        provider = Web3.WebsocketProvider(apiURL)
        webSocket = True
    elif apiURL[0:4] == "http":
        provider = Web3.HTTPProvider(apiURL)
        provider.middlewares.clear()
        webSocket = False
    elif apiURL[0] == "/":
        provider = Web3.IPCProvider(apiURL)
        webSocket = False
    else:
        print(f"apiUrl must start with wss, http or '/': {apiURL}")
        sys.exit(1)
    w3 = Web3(provider)
    return w3, webSocket  
  
class Worker(Process):
    def __init__(self, job_manager, apiUrl, id):
        super().__init__()
        self.job_manager = job_manager
        self.w3, self.websocket = getW3(apiUrl)
        self.id = id
        self.running = True
        atexit.register(self.stop)

    def run(self):
        while self.running:
            if not self.runCyclic():
                time.sleep(0.1)
                
    def runCyclic(self, wait = True):
            job = self.job_manager.getJob(self.id, wait = wait)
            if job:
                result = self.doJob(job)
                self.job_manager.addResult(job, result)
                return True
            else:
                return False

    def doJob(self, job):
        method, args, kwargs, _, _ = job
        if method == 'stop':
            self.running = False
            return
        attr = self.getW3Attr(method)
        if callable(attr):
            try:
                return attr(*args, **kwargs)
            except Exception as e:
                return e

    def getW3Attr(self, method):
        try:
            fn = getattr(self.w3, method)
        except AttributeError:
            fn = getattr(self.w3.eth, method)
        return fn

    def stop(self):
        self.running = False
        
class SharedResult(Logger):
    def __init__(self, manager,configPath, web3Settings, name= 'sr'):
        super().__init__(configPath, web3Settings, name)
        self.manager = manager
        self.lock = manager.RLock()
        self.list = manager.list()
    
    def append(self, val):
        with self.lock:
            self.list.append(val)
            
    def addResults(self, val):
        for _val in val:
            self.logDebug(f'sharedResultAdded: {blocks(_val)}')
        with self.lock:
            self.list+=val

    def clear(self):
        with self.lock():
            self.list.clear()
    
    def get(self, consume = True):
        with self.lock:
            if len(self.list)>0:
                if consume:
                    val = self.list
                    self.list = self.manager.list()
                    self.logInfo(f'result consumed {[blocks(x) for x in val]})')
                else:
                    val = copy.deepcopy(self.list)
                return val
            else:
                self.logInfo('no jobs ready')
                return []
            
            
        
class JobManager(Logger):
    def __init__(self, manager, configPath, web3Settings,name = 'jm'):
        super().__init__(configPath, web3Settings, name)
        self.manager = manager
        self.jobs = self.manager.list()
        self.completed = self.manager.list()
        self.lock = self.manager.RLock()

    #appends a new job into the back of the queue
    def addJob(self, method, *args, wait= True,target = None, **kwargs):
            self.logInfo(f'job added {method}')
            with self.lock:
                job_item = self.manager.list([ method, args, kwargs, target, None])
                self.jobs.append(job_item)
                # print(f'Job added: {method} {args} {kwargs}(Total jobs: {len(self.jobs)})')
            if wait:
                self.logInfo(f'waiting result {method}')
                return self.checkResult(job_item, wait = True)
            else:
                return job_item
    def addJobs(self, jobs):
        with self.lock:
            for job in jobs:
                job_item = self.manager.list(job)
                self.jobs.append(job_item)
            return self.jobs[-len(jobs):]
    #inserts a new job into the front of the queue
    def insertJob(self, index, method, *args, target = None, **kwargs):
            self.logInfo(f'job added {method}')
            with self.lock:
                job_item = self.manager.list([ method, args, kwargs, target, None])
                self.jobs.insert(0, job_item)
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
                if job[-1] is None and (job[-2]==None or job[-2] == target):
                    self.logInfo(f'job taken {blocks(self.jobs[jobIndex])}')
                    return self.jobs.pop(jobIndex)                   
        while wait:
            time.sleep(c.WAIT)
            with self.lock:
                for jobIndex, job in enumerate(self.jobs):
                    if job[-1] is None and (job[-2]==None or job[-2] == target):
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
            for i in range(len(jobs)):
                index = len(jobs)-1-i
                if jobs[index][-1] is not None:
                    completedJobs.insert(0,jobs.pop(index))
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
        
class PersistentProcessWrapper:
    def __init__(self, wrapped_class, *args, **kwargs):
        # Set up a pipe for communication between processes
        self.parent_conn, self.child_conn = multiprocessing.Pipe()
        # Store the class to be wrapped and its arguments
        self.wrapped_class = wrapped_class
        self.args = args
        self.kwargs = kwargs
        # Create and start the worker process
        self.process = multiprocessing.Process(target=self._worker)
        self.process.start()
        # Event loop executor for async method calls
        self.loop = asyncio.get_event_loop()

    def _worker(self):
        # Instantiate the wrapped class in the new process
        instance = self.wrapped_class(*self.args, **self.kwargs)
        while True:
            # Wait for a method call or terminate signal from the parent process
            method_name, method_args, method_kwargs = self.child_conn.recv()
            if method_name == '__terminate__':
                break
            # Execute the method and send the result back to the parent process
            method = getattr(instance, method_name)
            result = method(*method_args, **method_kwargs)
            self.child_conn.send(result)

    async def _async_method(self, method_name, *args, **kwargs):
        # Send method call asynchronously and await the result
        await self.loop.run_in_executor(
            None, self.parent_conn.send, (method_name, args, kwargs)
        )
        # Wait asynchronously for the result from the child process
        return await self.loop.run_in_executor(
            None, self.parent_conn.recv
        )

    def __getattr__(self, method_name):
        # Return an async function that calls the method in the child process
        async def async_method(*args, **kwargs):
            return await self._async_method(method_name, *args, **kwargs)
        return async_method

    async def close(self):
        # Send a termination signal to the child process asynchronously
        await self._async_method('__terminate__')
        # Wait for the process to terminate
        await self.loop.run_in_executor(None, self.process.join)

    def __del__(self):
        # Ensure the process is cleaned up when the object is destroyed
        asyncio.run(self.close())
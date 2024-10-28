from web3 import Web3
from multiprocessing import Manager
from multiprocessing import Manager
from rpc import RPC
import atexit
import copy
import time
from utils import getLastBlock
from logger import Logger
from configLoader import loadConfig
from hardhat import runHardhat
from multiprocessingUtils import Worker, SharedResult, JobManager, ContinuousWrapper  , getW3   
import asyncio
class MultiWeb3(Logger):
    @classmethod
    def createSharedResult(cls, manager, path='', config='', name = ''):
        return SharedResult(manager, name)
    @classmethod
    def createJobManager(cls,manager, path='', config=''):
        return JobManager(manager, path, config)
    
    def __init__(self, web3Settings, rpcSettings, configPath = '', name = 'mw3',manager = None, results = None, jobManager = None):
        super().__init__(name)
        self.configPath = configPath
        self.web3Settings=web3Settings
        if manager == None:
            self.manager = Manager()
        else:
            self.manager = manager
        self.numJobs = web3Settings['NUMJOBS']
        if results == None:
            self.results = SharedResult(self.manager)
        else:
            self.results = results
        self.worker = None
        if jobManager == None:
            self.jobManager = JobManager(self.manager)
        else:
            self.jobManager = jobManager
        self.processes = []
        self.rpcs = []
        self.lastResult = 0
        self.hardhatTargets = 0
        self.hardhats = []
        self.workerIndex=1
        self.lastBlock = 0
        self.running = True
        self.liveThreshold = 100
        self.usedRpcs = self.rpcs
        self.rpcTargets = 0
        for apiUrl, rpcSetting in rpcSettings.items():
            # worker = Worker(self.jobManager, apiUrl, self.workerIndex, self.results, rpcSetting["LOGNAMES"], self.manager, rpcSetting["POLLPERIOD"], len(rpcSettings))
            # if len(self.rpcs)==0 and self.web3Settings['MAINRPC']:
            #     self.worker = worker
            # else:
            #     self.processes.append(worker)
            #     worker.start()
            #     atexit.register(self.stopWorkers)
                self.addRpc(apiUrl, rpcSetting)
        atexit.register(self.stopWorkers)
            
    #add rpc and starts subprocess worker, if its hardhat stores it under self.hardhats, otherwise self.rpcs
    async def addRpc(self, apiUrl, rpcSetting):
        rpc = RPC(apiUrl, rpcSetting, self.jobManager, self.workerIndex) 
        #if apiUrl looks like local server, assume its connected to a hardhat instance
        if apiUrl[:17] == 'http://127.0.0.1:':  
            connected = self.jobManager.addJob('is_connected', target = rpc.id) 
            assert type(connected) != BaseException, 'cant connect to hardhat'
            self.hardhats.append(rpc)
            self.hardhatTargets+= self.workerIndex
        else:
            self.rpcs.append(rpc)
            self.rpcTargets+= self.workerIndex
        self.workerIndex<<=1    

    def stopWorkers(self):
        for i in range(len(self.processes)):
            self.jobManager.addJob('stop')
            

    # delegates arbitrary calls to jobmanager to be processed by worker processes          
    def __getattr__(self, name, override = True, target = None):
            if name in self.__dict__ or not override :
                return self.__dict__[name]
            elif name == 'eth' or name == 'account':
                return self
            # elif name == 'codec':
            #     return self.w3.codec  
            else:
                if self.worker and self.worker.id&target:
                    return self.worker.getW3Attr(name)
                else:
                    if not target:
                        target =self.rpcTargets
                    assert target != 0, 'no target for job'
                    def method(*args, **kwargs):
                        return self.jobManager.addJob(name, *args,  **kwargs) 
                    return method
#-------------------------------------eth.get_logs functions----------------------------------------------
    
    #behaves like get_logs but breaks the job up to multiple chunks which can be delegated to multiple rpcs
    # in a pararallel environment to speed up the process
    # if toBlock is 'current' will scan up to the current block upon start
    # if toBlock is 'latest' will continuously scan for blocks after latest block is reached
    # continuous=True assumes mw3 is in its own process, mw3.results must be polled to get the results
    # continuous=False assumes mget_logsCyclic will be called
    def setup_get_logs(self, filter, results = None, rpcs = None, callback = None):
        if rpcs != None:
            self.usedRpcs = rpcs
        else:
            self.usedRpcs = self.rpcs
        if results == None:
            self.results = SharedResult(self.manager)
        else:
            self.results = results
        self.live = False
        self.filter = copy.deepcopy(filter)
        self.remaining = [filter['fromBlock'], filter['toBlock']]    
        if self.remaining[1] == 'current':
            self.remaining[1] = self.get_block_number()
        if isinstance(self.remaining[1], int):
                return ContinuousWrapper(self.mGet_logsCyclic, rpcs = rpcs, callback = callback)
             
     # for running synchronously in the main process/thread, should be called cyclically after mget_logs
    def mGet_logsCyclic(self, callback = None):
        if self.worker:
            self.worker.runCyclic(wait=False)
        for rpc in self.usedRpcs:
                res = rpc.checkJobs()
                if res is not None and len(res)>0:  
                    self.lastBlock = max(getLastBlock(res[-1][-1]), self.lastBlock)                                 
                    self.results.addResults(res)  
                while len(rpc.jobs) <=self.numJobs and self.remaining[0] <= self.remaining[1]: 
                    rpc.takeJob(self.remaining, self.filter, callback = callback) 
                    
    # for running synchronously in the main process/thread when scanning to latest block,
    # should be called cyclically after mget_logs               
    def mGet_logsLatest(self, *args, **kwargs):
        if self.worker:
            self.worker.runCyclic(wait=False)
        for rpc in self.usedRpcs:
            self.logInfo(f'going live {rpc.apiUrl}')
            self.jobManager.addJob('live', self.filter,20,  target = rpc.id, wait = False, *args, **kwargs)

    # def sample(self, start, end, fraction, filter):
    #     import random
    #     import copy
    #     currentBlock = start
    #     filter = self.getFilter()
    #     i=0
    #     while currentBlock < end:
    #         _filter = copy.deepcopy(filter)
    #         filter['fromBlock'] = currentBlock
    #         filter['toBlock'] = end
    #         self.rpcs[i%len(self.rpcs)].
    
# if __name__ == '__main__':

#     from dotenv import load_dotenv
#     import os
#     import json

#     load_dotenv()
#     folderPath = os.getenv("FOLDER_PATH")
#     _configPath = f"{os.path.dirname(os.path.abspath(__file__))}/settings/{folderPath}/"
#     fileSettings, scanSettings, rpcSettings, rpcInterfaceSettings, hreSettings = (
#         loadConfig(_configPath + "/config.json")
#     )
#     mw3 = MultiWeb3(rpcSettings)
    

from web3 import Web3
from multiprocessing import Manager
from multiprocessing import Manager
from rpc import RPC
import atexit
import copy
import time
from logger import Logger
from configLoader import loadConfig
from hardhat import runHardhat
from multiprocessingUtils import Worker, SharedResult, JobManager, ContinuousWrapper

# class InvalidConfigException(Exception):
#     def __init__(self, text=""):
#         self.text = text
# wait_time = 0.01   
# def web3_worker(input_queue, output_queue, provider_url):
#     web3 = Web3(Web3.HTTPProvider(provider_url))
#     while True:
#         task = input_queue.get()
#         if task is None:  # A signal to terminate the process
#             break
#         method, args, kwargs = task
#         try:
#             result = getattr(web3.eth, method)(*args, **kwargs)
#             output_queue.put(result)
#         except Exception as e:
#             output_queue.put(e)    
# METHOD = 0
# ARGS= 1
# KWARGS=2
# TARGET=3
# RESULT=4         

class MultiWeb3(Logger):
    @classmethod
    def createSharedResult(cls, manager, path='', config='', name = ''):
        return SharedResult(manager, path, config, name)
    @classmethod
    def createJobManager(cls,manager, path='', config='', name = ''):
        return JobManager(manager, path, config)
    
    def __init__(self, web3Settings, rpcSettings, configPath = '',  manager = None, results = None, jobManager = None):
        super().__init__('mw3')
        self.configPath = configPath
        self.web3Settings=web3Settings
        if manager == None:
            self.manager = Manager()
        else:
            self.manager = manager
        self.numJobs = web3Settings['NUMJOBS']
        if results == None:
            self.results = SharedResult(self.manager, self.configPath, self.web3Settings)
        else:
            self.results = results
        self.worker = None
        if jobManager == None:
            self.jobManager = JobManager(self.manager, configPath, web3Settings)
        else:
            self.jobManager = jobManager
        self.processes = []
        self.rpcs = []
        self.lastResult = 0
        self.hardhats = []
        workerIndex=1
        self.lastBlock = 0
        self.running = True
        self.liveThreshold = 100
        self.usedRpcs = self.rpcs
        if isinstance(rpcSettings, dict):
            for apiUrl, rpcSetting in rpcSettings.items():
                rpc = RPC(apiUrl, rpcSetting, self.jobManager, workerIndex, configPath) 
                if len(self.rpcs)==0 and web3Settings['MAINRPC']:
                    self.worker = Worker(self.jobManager, apiUrl, workerIndex, self.results, rpcSettings["LOGNAMES"])
                    workerIndex<<=1 
                else:
                    worker = Worker(self.jobManager, apiUrl, workerIndex, self.results, rpcSetting["LOGNAMES"])
                    self.processes.append(worker)
                    worker.start()
                    atexit.register(self.stopWorkers)
                    workerIndex<<=1  
                self.rpcs.append(rpc)
            
        # elif isinstance(rpcSettings, str):
        #     rpcSettings = [rpcSettings]
            
        #     rpc = RPC(apiUrl, rpcSetting, self.jobManager, workerCount, configPath) 
        #     if not self.ownRpc or len(self.rpcs>0):
        #         worker = Worker(self.jobManager, apiUrl, workerCount)
        #         self.processes.append(worker)
        #         worker.start()
        #         atexit.register(self.stopWorker, workerCount)
        #         workerCount+=1
                
        # elif isinstance(rpcSettings, list):
        #     for apiUrl in rpcSettings:
        #         # rpc = RPC(rpcSetting, self.jobManager) 
        #         # self.rpcs.append(rpc)
        #         # if type(rpcSettings["APIURL"]) == dict:
        #         #     apiUrl = self.initHREW3(configPath, rpcSettings["APIURL"])
        #         #     self.hardhats.append(workerId)
        #         # else:
        #         #     apiUrl = rpcSettings["APIURL"]
        #         worker = Worker(self.jobManager, apiUrl, workerIndex)
        #         self.processes.append(worker)
        #         worker.start()
        #         workerIndex+=1
        #         atexit.register(self.stopWorkers)
        else:
            assert False, 'wrong config for rpcSettings'
    def getRpcs(self, hardhats = False):
        rpcs= []
        if hardhats:
            return self.rpcs
        for rpc in self.rpcs:
            if rpc not in self.hardhats or hardhats:
                rpcs.append(rpc)
        return rpcs
    
    def getHardhats(self):
        return self.hardhats   

    def stopWorkers(self):
        for i in range(len(self.processes)):
            self.jobManager.addJob('stop')
            

    # delegates arbitrary calls to jobmanager to be processed by worker processes          
    def __getattr__(self, name, override = True):
            if name in self.__dict__ or not override :
                return self.__dict__[name]
            elif name == 'eth':
                return self
            elif name == 'codec':
                if self.worker:
                    return self.worker.w3.codec
                else:
                    return self.processes[0].w3.codec   
            else:
                if self.worker:
                    return self.worker.getW3Attr(name)
                else:
                    def method(*args, **kwargs):
                        return self.jobManager.addJob(name, *args, **kwargs)
                    return method
#-------------------------------------eth.get_logs functions----------------------------------------------
    
    #behaves like get_logs but breaks the job up to multiple chunks which can be delegated to multiple rpcs
    # in a pararallel environment to speed up the process
    # if toBlock is current will scan up to the current block upon start
    # if toBlock is latest will continuously scan to latest block
    # continuous=True assumes mw3 is in its own process, mw3.results must be polled to get the results
    # continuous=False assumes mget_logsCyclic will be called
    def setup_get_logs(self, filter, results = None, rpcs = None):
        if rpcs != None:
            self.usedRpcs = rpcs
        else:
            self.usedRpcs = self.rpcs
        if results == None:
            self.results = SharedResult(self.manager, self.configPath, self.web3Settings)
        else:
            self.results = results
        self.live = False
        self.filter = copy.deepcopy(filter)
        self.remaining = [filter['fromBlock'], filter['toBlock']]    
        if self.remaining[1] == 'current':
            self.remaining[1] = self.get_block_number()
        if isinstance(self.remaining[1], int):
                return ContinuousWrapper(self.mGet_logsCyclic, rpcs = rpcs)
        # elif filter['toBlock'] == 'latest':
        #     return (self.mGet_logsLatest, rpcs = rpcs)
             
     # for running synchronously in the main process/thread, should be called cyclically after mget_logs
    def mGet_logsCyclic(self):
        if self.worker:
            self.worker.runCyclic(wait=False)
        for rpc in self.usedRpcs:
                res = rpc.checkJobs()
                if res is not None and len(res)>0:  
                    self.lastBlock = max(self.getLastBlock(res[-1][-1]), self.lastBlock)                                 
                    self.results.addResults(res)  
                while len(rpc.jobs) <=self.numJobs and self.remaining[0] <= self.remaining[1]: 
                    rpc.takeJob(self.remaining, self.filter) 
                    
    # for running synchronously in the main process/thread when scanning to latest block,
    # should be called cyclically after mget_logs               
    def mGet_logsLatest(self):
        if self.worker:
            self.worker.runCyclic(wait=False)
        for rpc in self.usedRpcs:
            self.logInfo(f'going live {rpc.apiUrl}')
            self.jobManager.addJob('live', self.filter,20,  target = rpc.id, wait = False)
            # self.logInfo(f'checking job for {rpc.apiUrl}')
            # res = rpc.checkJobs()
            # if res is not None and len(res)>0:  
            #     self.lastBlock = max(self.getLastBlock(res[-1][-1])+1, self.lastBlock)  
            #     rpc.lastBlock = self.lastBlock                               
            #     self.results.addResults(res) 
            #     rpc.lastTime = time.time()
            #     self.logInfo(f'livescan latest block received: {self.getLastBlock(res[-1][-1])+1} {self.getLastBlock(res[0][0])+1}')
            #     rpc.removeJobs(['get_logs'])
            #     rpc.takeJob([self.lastBlock, self.lastBlock+1], self.filter) 
            # elif rpc.lastBlock != self.lastBlock or time.time()-rpc.lastTime > 60:
            #     rpc.removeJobs(['get_logs'])
            #     rpc.takeJob([self.lastBlock, self.lastBlock+1], self.filter) 
    
    def getLastBlock(self, eventData):
        if len(eventData)>0:
            return eventData[-1]['blockNumber']  
        else:
            return 0
    
    #-------------------------- Hardhat configuration functions --------------------------
    def initHREW3(self, rpc, configPath, HRESettings):
        rpc.hh = runHardhat(configPath, HRESettings)
        if "--port" in HRESettings:
            port = f'http://127.0.0.1:{HRESettings["port"]}'
        else:
            port = "http://127.0.0.1:8454"
        self.logInfo(f"hardhat running on port {port}", True)    
        return {"APIURL": port}
if __name__ == '__main__':
    # mw3 = MultiWeb3(["https://base.llamarpc.com", "https://base.meowrpc.com"])
    # bn = mw3.eth.get_block_number()
    # print(bn)
    # codec = mw3.codec
    # print(codec)
    from dotenv import load_dotenv
    import os
    import json

    load_dotenv()
    folderPath = os.getenv("FOLDER_PATH")
    _configPath = f"{os.path.dirname(os.path.abspath(__file__))}/settings/{folderPath}/"
    fileSettings, scanSettings, rpcSettings, rpcInterfaceSettings, hreSettings = (
        loadConfig(_configPath + "/config.json")
    )
    mw3 = MultiWeb3(rpcSettings)
    

from web3._utils.events import get_event_data
import sys
from web3 import Web3
import time
import re
from logger import Logger
import math
import asyncio
import traceback
from hardhat import runHardhat
import copy
from multiprocessing.managers import ListProxy, DictProxy
from utils import blocks, toNative
from constants import c
    
def getW3(cfg):
    apiURL = cfg["APIURL"]
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




def getEventParameters(param):
    if "event" in param:
        event = str(param["event"]) + " " + str(param["logIndex"])
    else:
        event = "unkown " + str(param["logIndex"])

    return (
        param["blockNumber"],
        param["transactionHash"].hex(),
        param["address"],
        event,
    )



class RPC(Logger):

    def __init__(self, apiUrl, rpcSettings, jobManager, id):
        self.apiUrl = apiUrl
        super().__init__('rpc')
        self.maxChunkSize = rpcSettings["MAXCHUNKSIZE"]
        self.currentChunkSize = rpcSettings["STARTCHUNKSIZE"]
        self.eventsTarget = rpcSettings["EVENTSTARGET"]
        self.jobs = []
        self.failCount = 0
        self.jobManager = jobManager
        self.id = id
        self.lastBlock = 0
        self.lastTime = 0
        Logger.setProcessName(apiUrl)



    #takes a eth.get_logs job based on its scan parameters and the remaining range to be scanned
    def takeJob(self, remaining, filter, consume = True, callback = None):
            startBlock = remaining[0]
            endBlock = min(remaining[0] + self.currentChunkSize, remaining[1])
            if consume:
                remaining[0] = endBlock + 1
            newJob = (startBlock, endBlock)
            rpcFilter = filter.copy()
            rpcFilter['fromBlock'] = startBlock
            rpcFilter['toBlock'] = endBlock
            self.addGetLogsJob(rpcFilter, callback = callback)
 
    #wraps a filter into a get_logs job and adds it to the job manager
    def addGetLogsJob(self, rpcFilter, callback = None):
            self.jobs.append(self.jobManager.addJob('get_logs', rpcFilter, callback=callback, target = self.id, wait=False))
    
    #checks for any completed jobs from the worker and returns them, handles rpc errors
    def checkJobs(self, handleErrors = True):
        succsessfulJobs=[]
        completedJobs = self.jobManager.checkJobs(self.jobs)
        for job in completedJobs:
            if isinstance(job[-1], BaseException):
                if handleErrors:
                    self.logInfo(f'error with job {blocks(job)} {job[-1]} {self.apiUrl}')
                    self.handleError(job)
            else:
                self.logInfo(f'successful job: {blocks(job)}')
                succsessfulJobs.append(job)
        if len(succsessfulJobs)>0:
            lastJob = succsessfulJobs[-1]
            self.throttle(lastJob[-1], lastJob[1][0]['toBlock'] - lastJob[1][0]['fromBlock'])
        return succsessfulJobs

    #removes all jobs matching the method for this rpc from both local list and jobmanager, returns the removed jobs 
    def popJobs(self, methods):
        removedJobs = self.jobManager.popAllJobs(methods, self.id)
        jobsLength = len(self.jobs)
        for i in range(jobsLength):
            j = jobsLength-i-1
            if self.jobs[j] in removedJobs:
                self.jobs.pop(j)
        return removedJobs

    #throttles the block scan range depending on the configured target number of events
    def throttle(self, events, blockRange):
        if len(events) > 0:
            ratio = self.eventsTarget / (len(events))
            targetBlocks = math.ceil(ratio * blockRange)
            self.currentChunkSize = min(targetBlocks, self.maxChunkSize)
            self.currentChunkSize = max(self.currentChunkSize, 1)
            self.logInfo(
                    f"processed events: {len(events)}, ({blockRange}) blocks, throttled to {self.currentChunkSize}"
                )
    #-----------------------rpc error handling------------------------------------
    
    def handleRangeTooLargeError(self, failingJob):
        try:
            for word in failingJob[-1].args[0]["message"].split(' '):
                word = (word.replace('k', '000'))
                if word[0].isdigit():
                    maxBlock = int(word)
                    if maxBlock > self.currentChunkSize:
                        raise Exception
                    else:
                        filter = failingJob[1][0]
                        self.maxChunk = maxBlock
                        self.currentChunkSize = maxBlock                                
        except Exception as error:
                    self.maxChunk = int(self.currentChunkSize * 0.95)
                    self.currentChunkSize = min(self.maxChunk, self.currentChunkSize )
        self.splitJob(
                    math.ceil((filter['toBlock']-filter['fromBlock']) / maxBlock), failingJob
                )
        self.logInfo(f"blockrange too wide, reduced max to {self.currentChunkSize}")
    def handleInvalidParamsError(self, failingJob):
        if "Try with this block range" in failingJob[-1].args[0]["message"]:
            match = re.search(
                r"\[0x([0-9a-fA-F]+), 0x([0-9a-fA-F]+)\]", failingJob[-1].args[0]["data"]
            )
            if match:
                start_hex, end_hex = match.groups()
                suggestedLength = int(end_hex, 16) - int(start_hex, 16)
                self.logInfo(
                    f"too many events, suggested range {suggestedLength}"
                )
                self.splitJob(
                    math.ceil(self.currentChunkSize / suggestedLength), failingJob
                )
            else:
                self.logWarn(
                    f"unable to find suggested block range, splitting jobs"
                )
                self.splitJob(2, failingJob)
    def handleResponseSizeExceeded(self, failingJob):
        self.eventsTarget = self.eventsTarget*0.95
        self.splitJob(2, failingJob)
    def handleError(self, failingJob):
        if failingJob[0] == 'get_logs':
            self.logInfo('get logs error')
            e = failingJob[-1]
            if type(e) == ValueError:
                if e.args[0]["message"] == "block range is too wide" or 'range is too large' in e.args[0]["message"] :
                    self.handleRangeTooLargeError(failingJob)
                elif e.args[0]["message"] == "invalid params" or "response size should not greater than" in e.args[0]["message"]:
                    self.handleInvalidParamsError(failingJob)
                elif "response size exceed" in e.args[0]["message"]:
                    self.handleResponseSizeExceeded(failingJob)
                    
                elif e.args[0]["message"] == "rate limit exceeded":
                    self.logInfo(f"rate limited trying again")     
                else:
                    self.logWarn(
                        f"unhandled error {type(e), e}, {traceback.format_exc()} splitting jobs",
                        True,
                    )
                    self.splitJob(2, failingJob)
                    self.failCount += 1
            elif type(e) == asyncio.exceptions.TimeoutError:
                
                self.logInfo(f"timeout error, splitting jobs")
                self.splitJob(2, failingJob)
                self.failCount += 1
            elif type(e) == KeyboardInterrupt:
                pass
            else:
                self.logWarn(
                    f"unhandled error {type(e), e},{traceback.format_exc()}  splitting jobs",
                    True,
                )
                self.splitJob(2, failingJob)
                self.failCount += 1
        else:
            self.logWarn(
                    f"unhandled error {type(e), e},{traceback.format_exc()}  splitting jobs",
                    True,
                )
    # reduces the scan range by a specified factor, 
    # removes all jobs in the jobmanager, splits them based on the new scan range and adds them back
    def splitJob(self,numJobs, failingJob ,chunkSize =None, reduceChunkSize=True):
        self.logInfo(f'spitting jobs due to {blocks(failingJob)}')
        if type(chunkSize) !=(int):
            chunkSize = math.ceil((failingJob[1][0]['toBlock'] - failingJob[1][0]['fromBlock']) / numJobs)
        if reduceChunkSize:
            self.currentChunkSize = max(chunkSize, 1)
        removedJobs = self.popJobs(['get_logs'])
        removedJobs.insert(0, failingJob)
        newJobs = []
        for job in (removedJobs):
            self.logInfo(f'spitting jobs {blocks(failingJob)}')
            filter = job[1][0]
            currentBlock = filter['fromBlock'] 
            while currentBlock <= job[1][0]['toBlock']:
                _filter = copy.deepcopy(filter)
                _filter['fromBlock'] = currentBlock
                _filter['toBlock'] = min(currentBlock+chunkSize, filter['toBlock'])
                newJobs.append(['get_logs', (_filter,),  {},self.id, None])
                currentBlock = _filter['toBlock'] + 1
                self.logInfo(f'added job {blocks(job)}')
        self.jobs += self.jobManager.addJobs(newJobs)





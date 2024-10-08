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

    def __init__(self, apiUrl, rpcSettings, jobManager, id, configPath):
        self.apiUrl = apiUrl
        super().__init__(configPath, rpcSettings, 'rpc')
        self.maxChunkSize = rpcSettings["MAXCHUNKSIZE"]
        self.currentChunkSize = rpcSettings["STARTCHUNKSIZE"]
        self.eventsTarget = rpcSettings["EVENTSTARGET"]
        self.pollInterval = rpcSettings["POLLINTERVAL"]
        self.jobs = []
        self.failCount = 0
        self.activeStates = rpcSettings["ACTIVESTATES"]
        self.jobManager = jobManager
        self.id = id

    def checkJobs(self, handleErrors = True):
        succsessfulJobs=[]
        
        completedJobs = self.jobManager.checkJobs(self.jobs)
        for job in completedJobs:
            if isinstance(job[-1], BaseException):
                if handleErrors:
                    self.logInfo(f'error with job {blocks(job)}')
                    self.handleError(job)
            else:
                self.logInfo(f'successful job: {blocks(job)}')
                succsessfulJobs.append(job)
        if len(succsessfulJobs)>0:
            lastJob = succsessfulJobs[-1]
            self.throttle(lastJob[-1], lastJob[1][0]['toBlock'] - lastJob[1][0]['fromBlock'])
        return succsessfulJobs

                
    def takeJob(self, remaining, filter, consume = True):
            startBlock = remaining[0]
            endBlock = min(remaining[0] + self.currentChunkSize, remaining[1])
            if consume:
                remaining[0] = endBlock + 1
            newJob = (startBlock, endBlock)
            rpcFilter = filter.copy()
            rpcFilter['fromBlock'] = newJob[0]
            rpcFilter['toBlock'] = newJob[1]
            self.addGetLogsJob(rpcFilter)
            
    def removeJobs(self, methods):
        jobsLength = len(self.jobs)
        for i in range(jobsLength):
            j = jobsLength-i-1
            if self.jobs[j][c.TARGET]&self.id and self.jobs[j][c.METHOD] in methods:
                self.jobs.pop(j)
        return self.jobManager.popAllJobs(['get_logs'], self.id)

            
    def addGetLogsJob(self, rpcFilter):
            self.jobs.append(self.jobManager.addJob('get_logs', rpcFilter, target = self.id, wait=False))
            self.logInfo(f"job added: {blocks(rpcFilter)} ") 

    #---------------------event Decoding functions----------------------------------
    def decodeEvents(self, events):
        decodedEvents = []
        if self.scanMode == "ANYEVENT":
            for event in events:
                evt = get_event_data(
                    self.w3.codec,
                    self.contracts[event["address"]][event["topics"][0].hex()],
                    event,
                )
                decodedEvents.append(evt)
        elif self.scanMode == "ANYCONTRACT":
            for event in events:
                eventLookup = self.abiLookups[event["topics"][0].hex()]
                numTopics = len(event["topics"])
                if numTopics in eventLookup:
                    evt = get_event_data(
                        self.w3.codec,
                        self.abiLookups[event["topics"][0].hex()][numTopics],
                        event,
                    )
                    decodedEvents.append(evt)
        return self.getEventData(decodedEvents)
    
    def getEventData(self, events):
        decodedEvents = {}
        for param in events:
            blockNumber, txHash, address, index = getEventParameters(param)
            if blockNumber not in decodedEvents:
                decodedEvents[blockNumber] = {}
            if txHash not in decodedEvents[blockNumber]:
                decodedEvents[blockNumber][txHash] = {}
            if address not in decodedEvents[blockNumber][txHash]:
                decodedEvents[blockNumber][txHash][address] = {}
            decodedEvents[blockNumber][txHash][address][index] = {}
            for eventName, eventParam in param["args"].items():
                decodedEvents[blockNumber][txHash][address][index][
                    eventName
                ] = eventParam
        return decodedEvents

    def throttle(self, events, blockRange):
        if len(events) > 0:
            ratio = self.eventsTarget / (len(events))
            targetBlocks = math.ceil(ratio * blockRange)
            self.currentChunkSize = min(targetBlocks, self.maxChunkSize)
            self.currentChunkSize = max(self.currentChunkSize, 1)
            self.logInfo(
                    f"processed events: {len(events)}, ({blockRange}) blocks, throttled to {self.currentChunkSize}"
                )
    #-----------------------error handling------------------------------------
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
        if "Try with this block range" in failingJob[-1].args[0]["data"]:
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

    def splitJob(self,numJobs, failingJob ,chunkSize =None, reduceChunkSize=True):
        if type(chunkSize) !=(int):
            chunkSize = math.ceil((failingJob[1][0]['toBlock'] - failingJob[1][0]['fromBlock']) / numJobs)
        if reduceChunkSize:
            self.currentChunkSize = max(chunkSize, 1)
        removedJobs = self.removeJobs(['get_logs'])
        removedJobs.insert(0, failingJob)
        newJobs = []
        for job in (removedJobs):
            filter = job[1][0]
            currentBlock = filter['fromBlock'] 
            while currentBlock <= job[1][0]['toBlock']:
                _filter = copy.deepcopy(filter)
                _filter['fromBlock'] = currentBlock
                _filter['toBlock'] = min(currentBlock+chunkSize, filter['toBlock'])
                newJobs.append(['get_logs', (_filter,),  {},self.id, None])
                currentBlock = _filter['toBlock'] + 1
        self.jobs += self.jobManager.addJobs(newJobs)




from web3._utils.events import get_event_data
import sys
from web3 import Web3
import time
import re
from logger import Logger
import math
import asyncio

def getW3(cfg):
    apiURL = cfg["APIURL"]
    timeout = cfg["TIMEOUT"]
    if apiURL[0:3] == "wss":
        provider = Web3.WebsocketProvider(apiURL, websocket_timeout=5)
        webSocket = True
    elif apiURL[0:4] == "http":
        provider = Web3.HTTPProvider(apiURL)
        provider.middlewares.clear()
        webSocket = False
    elif apiURL[0] == "/":
        provider = Web3.IPCProvider(apiURL)
        webSocket = False
    else:
        print(f"error with URL {apiURL}")
        sys.exit(1)
    w3 = Web3(provider)
    return w3, webSocket


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
            decodedEvents[blockNumber][txHash][address][index][eventName] = eventParam
    return decodedEvents


def decodeEvents(self, events):
    decodedEvents = {}
    for param in events:
        blockNumber, txHash, address, index = getEventParameters(param)
        if blockNumber not in self.log:
            self.log[blockNumber] = {}
        if txHash not in self.log[blockNumber]:
            self.log[blockNumber][txHash] = {}
        if address not in decodedEvents:
            decodedEvents[address] = {}
        if address not in self.log[blockNumber][txHash]:
            self.log[blockNumber][txHash][address] = {}
        self.log[blockNumber][txHash][address][index] = {}
        decodedEvents[address][index] = {}
        for eventName, eventParam in param["args"].items():
            decodedEvents[address][index][eventName] = eventParam
            self.log[blockNumber][txHash][address][index][eventName] = eventParam
        self.saveState()
    return decodedEvents


def getEventParameters(param):
    return (
        param["blockNumber"],
        param["transactionHash"].hex(),
        param["address"],
        str(param["event"]) + " " + str(param["logIndex"]),
    )


class RPC(Logger):
    def __init__(self, rpcSettings,eventScanner, jobs=[], results={}):
        self.apiUrl = rpcSettings["APIURL"]
        super().__init__(self.apiUrl, rpcSettings["DEBUG"])
        self.w3, self.websocket = getW3(rpcSettings)
        self.maxChunkSize = rpcSettings["MAXCHUNKSIZE"]
        self.currentChunkSize = rpcSettings["STARTCHUNKSIZE"]
        self.throttleFactor = rpcSettings["THROTTLEFACTOR"]
        self.throttleAmount = rpcSettings["THROTTLEAMOUNT"]
        self.eventsTarget = rpcSettings["EVENTSTARGET"]
        self.es = eventScanner
        self.jobTime = time.time()
        self.jobs = []
        self.currentResults = {}
        self.debugQueue = []
        self.start = 0
        self.end = 0
        self.running = True
        self.debug = rpcSettings["DEBUG"]
        self.logInfo(f'logging enabled')
        self.state = 0
        if len(self.jobs) > 0:
            print('error')
        

    def run(self):
        while self.running:
            if len(self.jobs) > 0:
                if self.start == 0:
                    print('error')
                try:
                    job = self.getJob()
                    self.logInfo(f'starting job {job}')
                    events = self.scanChunk(job[0], job[1])
                    self.currentResults.update(self.getEventData(events))
                    self.throttle(events, self.jobs[0][1] - self.jobs[0][0])
                    self.logInfo(
                        f"processed events: {len(events)}, from {self.jobs[0][0]} to {self.jobs[0][1]}, throttled to {self.currentChunkSize}"
                    )
                    self.jobs.pop(0)
                except Exception as e:
 
                    self.handleError(e)
            else:
                self.state = 2

    def getResults(self):
        self.state = 0
        self.logInfo(f'results extracted {self.start} to {self.end}')
        return self.currentResults, self.start,  self.end
    
    def addJob(self, startBlock, endBlock):
        self.state = 1
        end = min(endBlock, startBlock + self.currentChunkSize)
        self.jobs.append((startBlock, end))
        self.start = startBlock
        self.end = end
        self.logInfo(f"job added: {self.jobs} ")
        return end
    
    def getJob(self):
        length = self.jobs[0][1]-self.jobs[0][0]
        if length >self.currentChunkSize:
            self.logInfo(f"existing job too big, splitting")
            self.splitJob(math.ceil(length/self.currentChunkSize))
        return self.jobs[0]
        
        
    def scanChunk(self, start, end):
        allEvents = []
        filterParams = self.getFilter(start, end)
        eventlogs = self.w3.eth.get_logs(filterParams)
        if self.es.mode == "ANYEVENT":
            for eventLog in eventlogs:
                evt = get_event_data(
                    self.w3.codec,
                    self.es.contracts[eventLog["address"]][eventLog["topics"][0].hex()],
                    eventLog,
                )
                allEvents.append(evt)
        elif self.es.mode == "ANYCONTRACT":
            for eventLog in eventlogs:
                eventLookup = self.es.abiLookups[eventLog["topics"][0].hex()]
                numTopics = len(eventLog["topics"])
                if numTopics in eventLookup:
                    evt = get_event_data(
                        self.w3.codec, self.es.abiLookups[eventLog["topics"][0].hex()][numTopics], eventLog
                    )
                    allEvents.append(evt)
        
        return allEvents

    def getFilter(self, start, end):
        if self.es.mode == "ANYEVENT":
            return {
                "fromBlock": start,
                "toBlock": end,
                "topics": [],
                "address": list(self.es.contracts.keys()),
            }
        elif self.es.mode == "ANYCONTRACT":
            return {
                "fromBlock": start,
                "toBlock": end,
                "topics": [list(self.es.abiLookups.keys())],
                "address": [],
            }

    def throttle(self, events, blockRange):
        if len(events) > 0:
            ratio = self.eventsTarget / (len(events))
            targetBlocks = int(ratio * blockRange)
            self.currentChunkSize = targetBlocks
            
    def getFactor(self,current, target):
        factor = 1
        while current/factor>target:
            factor += 1
        return factor
    
    def handleError(self, e):
        if type(e) == ValueError:
            if e.args[0]["message"] == "block range is too wide":
                self.maxChunk = int(self.currentChunkSize * 0.98)
                self.currentChunkSize = self.maxChunk
                self.logInfo(
                    f"blockrange too wide, reduced max to {self.maxChunk}"
                )
            elif e.args[0]["message"] == 'invalid params':
                if 'Try with this block range' in e.args[0]["data"]:
                    match = re.search(r'\[0x([0-9a-fA-F]+), 0x([0-9a-fA-F]+)\]', e.args[0]["data"])
                    if match:
                        start_hex, end_hex = match.groups()
                        suggestedLength = int(end_hex, 16)-int(start_hex, 16)
                        self.logInfo(
                    f"too many events, suggested range {suggestedLength}"
                )
                        self.splitJob(math.ceil(self.currentChunkSize / suggestedLength))
                    else:
                        self.logWarn(f"unable to find suggested block range, splitting jobs")
                        self.splitJob(2)
            elif e.args[0]["message"] == "rate limit exceeded":
                self.logInfo(f"rate limited trying again")
                
        elif type(e) == asyncio.exceptions.TimeoutError:
            self.logInfo(f"timeout error, splitting jobs")
            self.splitJob(2)
        else:
            self.logWarn(f"unhandled error {type(e), e} splitting jobs")
            self.splitJob(2)

    def splitJob(self, numJobs, reduceChunkSize = True):
        oldJob = self.jobs[0]
        chunkSize = math.ceil((oldJob[1]-oldJob[0])/numJobs)+1
        if reduceChunkSize:
            self.currentChunkSize = chunkSize            
        current = oldJob[0]
        newJobs = []
        for _ in range(numJobs):
            newJobs.append((current, current+chunkSize))
            current +=chunkSize
        self.logInfo(f"splitting Job {self.jobs[0]} to {chunkSize} blocks: {newJobs}")
        self.jobs= newJobs+self.jobs[1:]

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

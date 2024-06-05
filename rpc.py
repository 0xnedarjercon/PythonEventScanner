from web3._utils.events import get_event_data
import sys
from web3 import Web3
import time
import re
from logger import Logger
import math
import asyncio
import traceback


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
    return decodedEvents


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
    def __init__(self, rpcSettings, iScanner, scanMode, contracts, abiLookups):
        self.apiUrl = rpcSettings["APIURL"]
        super().__init__(self.apiUrl.split("./")[-1], rpcSettings["DEBUGLEVEL"])
        self.w3, self.websocket = getW3(rpcSettings)
        self.maxChunkSize = rpcSettings["MAXCHUNKSIZE"]
        self.currentChunkSize = rpcSettings["STARTCHUNKSIZE"]
        self.eventsTarget = rpcSettings["EVENTSTARGET"]
        self.pollInterval = rpcSettings["POLLINTERVAL"]
        self.iScanner = iScanner
        self.contracts = contracts
        self.abiLookups = abiLookups
        self.jobs = []
        self.failCount = 0
        self.activeStates = rpcSettings["ACTIVESTATES"]
        self.start = 0
        self.end = 0
        self.running = True
        self.scanMode = scanMode
        self.logDebug(f"logging enabled")

    def run(self):
        state = self.iScanner.state
        while state > -1 and self.running == True:
            self.iScanner.checkSyncRequest(self)
            if state == 1 and state in self.activeStates:
                self.runFixed()
            elif state == 2 and state in self.activeStates:
                self.runLive()
            else:
                self.iScanner.checkSyncRequest(self, blocking=True)
            state = self.iScanner.state

    def runFixed(self):
        while self.iScanner.state == 1:
            self.iScanner.checkSyncRequest(self)
            self.fixedScan()

    def fixedScan(self):
        if len(self.jobs) == 0:
            newJob = self.iScanner.getScanJob(self.currentChunkSize)
            if newJob != [] and newJob[0] != newJob[1]:
                self.jobs.append(newJob)
                self.logInfo(f"job added: {self.jobs} ")
        else:
            try:
                job = self.nextJob()
                self.logInfo(f"starting job {job}")
                events = self.scanChunk(job[0], job[1])
                self.iScanner.addScanResults(
                    [job[0], self.decodeEvents(events), job[1]]
                )
                self.throttle(events, self.jobs[0][1] - self.jobs[0][0])
                self.logInfo(
                    f"processed events: {len(events)}, from {self.jobs[0][0]} to {self.jobs[0][1]} ({self.jobs[0][1]-self.jobs[0][0]}), throttled to {self.currentChunkSize}"
                )
                self.jobs.pop(0)
                self.failCount = 0
            except Exception as e:
                self.handleError(e)
                
    def runLive(self):
        start = self.iScanner.start
        self.logInfo(f"livescan started at block {start}")
        if self.websocket:
            self.filterParams = self.getFilter(start, "latest")
            self.filterParams = self.w3.eth.filter(self.filterParams)
            while self.iScanner.state == 2:

                start = self.iScanner.start
                try:
                    self.iScanner.checkSyncRequest(self)
                    startTime = time.time()
                    self.logInfo("request latest events")
                    newEvents = self.filterParams.get_new_entries()
                    if len(newEvents) > 0:
                        self.logInfo(f"updating results with {len(newEvents)} events")
                        self.iScanner.addLiveResults(self.decodeEvents(newEvents))
                    delta = time.time() - startTime
                    if delta < self.pollInterval:
                        time.sleep(self.pollInterval - delta)
                        self.failCount = 0
                except Exception as e:
                    self.logWarn(
                        f"error: {type(e)}, {e}, {traceback.format_exc()}", True
                    )
        else:
            while self.iScanner.state == 2:
                try:
                    self.iScanner.checkSyncRequest(self)
                    start = self.iScanner.start
                    self.logInfo(f"request new events from {start}")
                    self.filterParams = self.getFilter(start, start + 5)
                    startTime = time.time()
                    newEvents = self.w3.eth.get_logs(self.filterParams)
                    if len(newEvents) > 0:
                        self.logInfo(
                            f"updating results with {len(newEvents)} new events"
                        )
                        self.iScanner.addLiveResults(self.decodeEvents(newEvents))
                    delta = time.time() - startTime
                    if delta < self.pollInterval:
                        time.sleep(self.pollInterval - delta)
                    self.failCount = 0
                except Exception as e:
                    self.logWarn(
                        f"error: {type(e)}, {e}, {traceback.format_exc()}", True
                    )

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

    def nextJob(self):
        length = self.jobs[0][1] - self.jobs[0][0]
        if length > self.currentChunkSize + 1:
            self.logInfo(
                f"existing job too big, splitting {length}, {self.currentChunkSize}"
            )
            self.splitJob(math.ceil(length / self.currentChunkSize))
        return self.jobs[0]

    def scanChunk(self, start, end):
        filterParams = self.getFilter(start, end)
        eventlogs = self.w3.eth.get_logs(filterParams)
        self.logInfo(f"received events: {len(eventlogs)}")
        return eventlogs

    def getFilter(self, start, end):
        if self.scanMode == "ANYEVENT":
            return {
                "fromBlock": start,
                "toBlock": end,
                "topics": [],
                "address": list(self.contracts.keys()),
            }
        elif self.scanMode == "ANYCONTRACT":
            return {
                "fromBlock": start,
                "toBlock": end,
                "topics": [list(self.abiLookups.keys())],
                "address": [],
            }

    def throttle(self, events, blockRange):
        if len(events) > 0:
            ratio = self.eventsTarget / (len(events))
            targetBlocks = math.ceil(ratio * blockRange)
            self.currentChunkSize = targetBlocks
            self.currentChunkSize = max(self.currentChunkSize, 1)

    def getFactor(self, current, target):
        factor = 1
        while current / factor > target:
            factor += 1
        return factor

    def handleError(self, e):            
        if type(e) == ValueError:
            if e.args[0]["message"] == "block range is too wide":
                self.maxChunk = int(self.currentChunkSize * 0.98)
                self.currentChunkSize = self.maxChunk
                self.logInfo(f"blockrange too wide, reduced max to {self.maxChunk}")
            elif e.args[0]["message"] == "invalid params":
                if "Try with this block range" in e.args[0]["data"]:
                    match = re.search(
                        r"\[0x([0-9a-fA-F]+), 0x([0-9a-fA-F]+)\]", e.args[0]["data"]
                    )
                    if match:
                        start_hex, end_hex = match.groups()
                        suggestedLength = int(end_hex, 16) - int(start_hex, 16)
                        self.logInfo(
                            f"too many events, suggested range {suggestedLength}"
                        )
                        self.splitJob(
                            math.ceil(self.currentChunkSize / suggestedLength)
                        )
                    else:
                        self.logWarn(
                            f"unable to find suggested block range, splitting jobs"
                        )
                        self.splitJob(2)

            elif e.args[0]["message"] == "rate limit exceeded":
                self.logInfo(f"rate limited trying again")
                time.sleep(0.5)
            elif "response size should not greater than" in e.args[0]["message"]:
                self.logInfo(f"too much data, splitting job, {e}")
                self.splitJob(2)
            else:
                self.logWarn(
                    f"unhandled error {type(e), e}, {traceback.format_exc()} splitting jobs",
                    True,
                )
                self.splitJob(2)
                self.failCount+=1
        elif type(e) == asyncio.exceptions.TimeoutError:
            self.logInfo(f"timeout error, splitting jobs")
            self.splitJob(2)
            self.failCount+=1
        else:
            self.logWarn(
                f"unhandled error {type(e), e},{traceback.format_exc()}  splitting jobs",
                True,
            )
            time.sleep(0.5)
            self.splitJob(2)
            self.failCount+=1
        if self.failCount == 10:
            for job in self.jobs:
                self.iScanner.addScanRange(job[0], job[1])
        elif self.failCount >20:
            for job in self.jobs:
                self.iScanner.addScanRange(job[0], job[1])
            self.logCritical('too many failures, rpc shutting down')
            self.running = False
    def splitJob(self, numJobs, reduceChunkSize=True):
        oldJob = self.jobs[0]
        chunkSize = math.ceil((oldJob[1] - oldJob[0]) / numJobs)
        if reduceChunkSize:
            self.currentChunkSize = chunkSize
        current = oldJob[0]
        for i in range(numJobs):
            self.jobs.insert(1 + i, [current, current + chunkSize])
            current += chunkSize

        self.logInfo(
            f"split Job {self.jobs[0]} to {chunkSize} blocks: {self.jobs[1:1+numJobs]}"
        )
        self.jobs.pop(0)

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

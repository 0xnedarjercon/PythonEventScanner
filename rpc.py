from web3._utils.events import get_event_data
import sys
from web3 import Web3
import time


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


class RPC:
    def __init__(self, rpcSettings, eventScanner, debug=True):
        self.apiUrl = rpcSettings["APIURL"]
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
        self.d(f'logging info for {self.apiUrl}')

    def d(self, string):
        if self.debug:
            self.es.log.info(f'{self.apiUrl}: {string}')

    def run(self):
        while self.running:
            if len(self.jobs) > 0:
                try:
                    events = self.scanChunk(self.jobs[0][0], self.jobs[0][1])
                    self.currentResults.update(self.getEventData(events))
                    self.throttle(events, self.jobs[0][1] - self.jobs[0][0])
                    self.d(
                        f"processed events: {len(events)}, from {self.jobs[0][0]} to {self.jobs[0][1]}, throttled to {self.currentChunkSize}"
                    )
                    self.jobs.pop(0)
                except Exception as e:
                    self.handleError(e)

    def addJob(self, startBlock, endBlock):
        end = min(endBlock, startBlock + self.currentChunkSize)
        self.jobs.append((startBlock, end))
        self.start = startBlock
        self.end = end
        self.d(f"{self.apiUrl}: job added: {self.jobs} ")
        return end

    def scanChunk(self, start, end):
        allEvents = []
        filterParams = self.getFilter(start, end)
        logs = self.w3.eth.get_logs(filterParams)
        if self.es.mode == "ANYEVENT":
            for log in logs:
                evt = get_event_data(
                    self.w3.codec,
                    self.es.contracts[log["address"]][log["topics"][0].hex()]["abi"],
                    log,
                )
                allEvents.append(evt)
        elif self.es.mode == "ANYCONTRACT":
            for log in logs:
                if self.es.topicCounts[log["topics"][0].hex()] == len(log["topics"]):
                    evt = get_event_data(
                        self.w3.codec, self.es.abiLookups[log["topics"][0].hex()], log
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
            pass

    def throttle(self, events, blockRange):
        if len(events) > 0:
            ratio = self.eventsTarget / (len(events))
            targetBlocks = int(ratio * blockRange)
            self.currentChunkSize = targetBlocks

    def handleError(self, e):
        self.es.log.error(f"error: {e}")
        if type(e) == ValueError:
            if e.args[0]["message"] == "block range is too wide":

                self.maxChunk = int(self.currentChunkSize * 0.98)
                self.currentChunkSize = self.maxChunk
                self.d(
                    f"blockrange too wide, reduced max to {self.maxChunk}"
                )
            elif e.args[0]["message"] == "rate limit exceeded":
                self.d(f"rate limited trying again")
                return
        else:
            self.d(f"error {type(e), e} splitting jobs")
        self.splitJob()

    def splitJob(self):
        oldJob = self.jobs[0]
        self.currentChunkSize = int((oldJob[1] - oldJob[0]) / 2)
        self.jobs.append((oldJob[0], oldJob[0] + self.currentChunkSize))
        self.jobs.append([oldJob[0] + self.currentChunkSize, oldJob[1]])
        self.d(f"splitting Job {self.jobs[0]} to {(oldJob[0], oldJob[0] + self.currentChunkSize)}+{[oldJob[0] + self.currentChunkSize, oldJob[1]]}")
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

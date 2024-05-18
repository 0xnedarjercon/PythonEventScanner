from web3 import Web3
import time
from web3._utils.events import get_event_data
import sys
import json
from tqdm import tqdm
import os

directory = os.path.dirname(os.path.abspath(__file__))
print(directory)


from fileHandler import FileHandler

import asyncio


class Mode:
    MULTICONTRACT = 0
    MULTIEVENT = 1


def scan():
    es = EventScanner()
    try:
        es.scan()
    except KeyboardInterrupt:
        print("keyboard interrupt detected, saving...")
        es.saveState()
    return es.latestBlock


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


def getFileString(fileSettings, scanSettings):
    if fileSettings["FILESTRING"] == "":
        startBlock = scanSettings["STARTBLOCK"]
        endBlock = scanSettings["ENDBLOCK"]
        if scanSettings["MODE"] == "ANYEVENT":
            contracts = scanSettings["CONTRACTS"]
            fileString = f"Block {startBlock} to {endBlock} contracts {','.join(scanSettings['CONTRACTS'])}"
        elif scanSettings["MODE"] == "ANYCONTRACT":
            eventNames, eventSigs, abiLookups, topicCounts = abiToSig(abi)
            fileString = (
                f"Block {startBlock} to {endBlock} events {','.join(eventNames)}"
            )
    return fileString


def getScanParameters(configPath):
    rpcSettings, scanSettings, fileSettings = readConfig(configPath)
    startBlock = scanSettings["STARTBLOCK"]
    endBlock = scanSettings["ENDBLOCK"]
    w3, websocket = getW3(rpcSettings)
    if fileSettings["FILENAME"] == "":
        if scanSettings["MODE"] == "ANYEVENT":
            contracts = scanSettings["CONTRACTS"]
            fileString = f"Block {startBlock} to {endBlock} contracts {','.join(scanSettings['CONTRACTS'])}"
        elif scanSettings["MODE"] == "ANYCONTRACT":
            eventNames, eventSigs, abiLookups, topicCounts = abiToSig(
                scanSettings["ABI"]
            )
            fileString = (
                f"Block {startBlock} to {endBlock} events {','.join(eventNames)}"
            )
    if scanSettings["ENDBLOCK"] == "latest":
        endBlock = w3.eth.block_number
    return (
        w3,
        websocket,
        contracts,
        eventSigs,
        abi,
        fileString,
        saveInterval,
        startBlock,
        endBlock,
        maxChunk,
        abiLookups,
        topicCounts,
        scanSettings,
    )


def calcSigniature(event):
    eventName = event["name"]
    inputTypes = [input_abi["type"] for input_abi in event["inputs"]]
    eventSig = Web3.keccak(text=f"{eventName}({','.join(inputTypes)})").hex()
    # abiLookup = abi[x]
    topicCount = sum(1 for inp in event["inputs"] if inp["indexed"]) + 1
    return eventSig, topicCount


def processContracts(folder, inContracts):
    contracts = {}
    for contract, abiFile in inContracts.items():
        checksumAddress = Web3.to_checksum_address(contract)
        contracts[checksumAddress] = {}
        with open(f"{folder}ABIs/{abiFile}.json") as f:
            abi = json.load(f)
            for entry in abi:
                if entry["type"] == "event":
                    eventSig, topicCount = calcSigniature(entry)
                    contracts[checksumAddress][eventSig] = {
                        "abi": entry,
                        "topicCount": topicCount,
                    }
    return contracts


class EventScanner:

    def __init__(self):
        folderPath = os.getenv("FOLDER_PATH")
        self.configPath = f"{directory}/settings/{folderPath}/"
        rpcSettings, scanSettings, fileSettings = readConfig(self.configPath)
        self.loadRPCSettings(rpcSettings)
        self.loadScanSettings(scanSettings)
        self.loadFileSettings(fileSettings)

    def loadScanSettings(self, scanSettings):
        self.startBlock = scanSettings["STARTBLOCK"]
        if scanSettings["ENDBLOCK"] == "latest":
            self.endBlock = self.w3.eth.get_block_number()
        else:
            self.endBlock = scanSettings["ENDBLOCK"]
        self.mode = scanSettings["MODE"]
        self.events = scanSettings["EVENTS"]
        self.contracts = processContracts(self.configPath, scanSettings["CONTRACTS"])
        self.abis = scanSettings["ABI"]
        self.forceNew = scanSettings["FORCENEW"]

    def loadRPCSettings(self, rpcSettings):
        self.w3, self.websocket = getW3(rpcSettings)
        self.maxChunkSize = rpcSettings["MAXCHUNKSIZE"]
        self.startChunkSize = rpcSettings["STARTCHUNKSIZE"]
        self.throttleFactor = rpcSettings["THROTTLEFACTOR"]
        self.throttleAmount = rpcSettings["THROTTLEAMOUNT"]
        self.eventsTarget = rpcSettings["EVENTSTARGET"]

    def loadFileSettings(self, fileSettings):
        filePath = self.configPath + self.getFileString(fileSettings)
        self.fileHandler = FileHandler(
            filePath, fileSettings["MAXENTRIES"], fileSettings["SAVEINTERVAL"]
        )

    def getFileString(self, fileSettings):
        if fileSettings["FILENAME"] == "":
            startBlock = self.startBlock
            endBlock = self.endBlock
            if self.mode == "ANYEVENT":
                contracts = list(self.contracts.keys())
                fileString = (
                    f"Block {startBlock} to {endBlock} contracts {','.join(contracts)}"
                )
            elif self.mode == "ANYCONTRACT":
                fileString = (
                    f"Block {startBlock} to {endBlock} events {','.join(self.events)}"
                )
        else:
            fileString = fileSettings["FILENAME"]
        return fileString

    def initFromParams(
        self,
        w3,
        contracts,
        eventSigs,
        abis,
        fileString,
        saveinterval,
        folder,
        throttleAmount,
        mode=Mode.MULTICONTRACT,
        forceNew=False,
    ):

        self.w3 = w3
        self.throttleAmount = throttleAmount
        self.contracts = {}
        self.forceNew = forceNew
        self.contracts = processContracts(folder, contracts)
        # self.state = state
        self.eventSigs = eventSigs
        self.abis = abis
        self.abiLookups = {}
        self.topicCounts = {}
        self.latestBlock = 0
        self.saveInterval = saveinterval
        self.mode = mode
        for x in range(len(abis)):
            self.abiLookups[eventSigs[x]] = abis[x]
            self.topicCounts[eventSigs[x]] = (
                sum(1 for inp in abis[x]["inputs"] if inp["indexed"]) + 1
            )
        self.fileString = folder + "output/" + fileString + ".json"
        self.backupFile = folder + "output/" + fileString + "bkup.json"
        if not self.forceNew:
            self.initLog()
        else:
            self.log = {"latest": 0}
            self.latestBlock = 0

    def updateConfig(self, args):
        with open(self.configPath) as f:
            cfg = json.load(f)
        tmp = cfg
        for i in range(len(args)):
            if i < len(args) - 1:
                tmp = tmp[args[i]]
            else:
                tmp = args[i]
        with open(self.configPath) as f:
            f.write(json.dump(cfg, indent=4))

    def scanChunk(self, start, end):
        allEvents = []
        filterParams = {
            "fromBlock": start,
            "toBlock": end,
            "topics": [],
            "address": list(self.contracts.keys()),
        }
        logs = self.w3.eth.get_logs(filterParams)
        if self.mode == "ANYEVENT":
            for log in logs:
                evt = get_event_data(
                    self.w3.codec,
                    self.contracts[log["address"]][log["topics"][0].hex()]["abi"],
                    log,
                )
                allEvents.append(evt)
        elif self.mode == "ANYCONTRACT":
            for log in logs:
                if self.topicCounts[log["topics"][0].hex()] == len(log["topics"]):
                    evt = get_event_data(
                        self.w3.codec, self.abiLookups[log["topics"][0].hex()], log
                    )
                    allEvents.append(evt)
        self.latestBlock = end
        return allEvents

    def getLastScannedBlock(self):
        return self.fileHandler.latest

    def scan(self):
        print(
            f"starting scan at {time.asctime(time.localtime(time.time()))}, scanning to {self.endBlock}"
        )
        start = max([self.startBlock, self.fileHandler.latest])
        startChunk = start
        endChunk = 0
        startTime = time.time()

        totalBlocks = self.endBlock - start
        self.currentChunkSize = self.startChunkSize
        numChunks = 0
        with tqdm(total=start - self.endBlock) as progress_bar:
            while endChunk < self.endBlock and (self.endBlock > start):
                try:
                    endChunk = min(
                        [self.endBlock, startChunk + int(self.currentChunkSize)]
                    )
                    events = self.scanChunk(startChunk, endChunk)
                    self.fileHandler.log(self.getEventData(events), endChunk)
                    self.throttle(events)
                    elapsedTime = time.time() - startTime
                    progress = (startChunk - start) / totalBlocks
                    if progress > 0:
                        eta = time.asctime(
                            time.localtime(
                                (1 - progress) * elapsedTime / progress + startTime
                            )
                        )
                    else:
                        eta = "INF"
                    progress_bar.set_description(
                        f"Current block: {startChunk} , blocks in a scan batch: {self.currentChunkSize}, events processed in a batch {len(events)} ETA:{eta}"
                    )
                    progress_bar.update(self.currentChunkSize)
                    startChunk = endChunk
                    numChunks += 1
                except Exception as e:
                    self.handleError(e)
                if self.currentChunkSize < 0:
                    print("cant reduce block size any more, try reducing thottleAmount")
                    self.saveState()
                    sys.exit()

        print(
            f"Scanned blocks {start}-{self.endBlock} from {time.asctime(time.localtime(startTime))} to {time.asctime(time.localtime(time.time()))} in {numChunks} chunks"
        )
        self.saveState()

    def throttleDown(self):
        self.currentChunkSize -= self.throttleAmount
        self.currentChunkSize = int(self.currentChunkSize / self.throttleFactor)

    def throttleup(self):
        self.currentChunkSize += self.throttleAmount
        self.currentChunkSize = int(self.currentChunkSize * self.throttleFactor)

    def throttle(self, events):
        ratio = (self.eventsTarget - len(events)) / self.eventsTarget + 1
        target = int(ratio * self.currentChunkSize)
        if target > self.currentChunkSize:
            self.currentChunkSize = int((target + self.currentChunkSize * 3) / 4)
        else:
            self.currentChunkSize = int((target * 3 + self.currentChunkSize) / 4)
        self.currentChunkSize = min(self.currentChunkSize, self.maxChunkSize)

    def handleError(self, e):
        if type(e) == ValueError:
            print("valueerror", e)
            if e.args[0]["message"] == "block range is too wide":
                maxChunk = int(self.currentChunkSize * 0.98)
                print(f"reduced max chunk size to {maxChunk}")
                successes = 0
                self.throttleDown()
                print(e, f"\nreducing scan size to {self.currentChunkSize} blocks")
            elif e.args[0]["message"] == "rate limit exceeded":
                print(f"rate limited trying again")
        else:
            self.throttleDown()
            print(e, f"\nreducing scan size to {self.currentChunkSize} blocks")

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

    def saveState(self):
        self.fileHandler.save()

    def backup(self):
        with open(os.getenv("CONFIG_PATH") + "output/backup.json", "w") as f:
            f.write(json.dumps(self.log, indent=4))


def getEventParameters(param):
    return (
        param["blockNumber"],
        param["transactionHash"].hex(),
        param["address"],
        str(param["event"]) + " " + str(param["logIndex"]),
    )


def abiToSig(abi):
    eventNames = []
    eventSigs = []
    abiLookups = {}
    topicCounts = {}
    for x, event in enumerate(abi):
        eventNames.append(event["name"])
        inputTypes = [input_abi["type"] for input_abi in event["inputs"]]
        eventSigs.append(
            Web3.keccak(text=f"{eventNames[-1]}({','.join(inputTypes)})").hex()
        )
        abiLookups[eventSigs[x]] = abi[x]
        topicCounts[eventSigs[x]] = (
            sum(1 for inp in abi[x]["inputs"] if inp["indexed"]) + 1
        )

    return eventNames, eventSigs, abiLookups, topicCounts


def readConfig(configPath):
    with open(configPath + "config.json") as f:
        cfg = json.load(f)
    return cfg["RPCSETTINGS"], cfg["SCANSETTINGS"], cfg["FILESETTINGS"]


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()
    scan()
# //  9550996,

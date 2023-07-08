from web3 import Web3
import time
from web3._utils.events import get_event_data
import sys
import json
from tqdm import tqdm


def scan(
    w3,
    contracts,
    eventSigs,
    abi,
    fileString,
    saveInterval,
    startBlock,
    endBlock,
    maxChunk,
    folder,
):
    es = EventScanner(w3, contracts, eventSigs, abi, fileString, saveInterval, folder)
    try:
        es.scan(startBlock, endBlock, maxChunk)
    except KeyboardInterrupt:
        print("keyboard interrupt detected, saving...")
        es.saveState()
    return es.latestBlock


def getW3(apiURL):
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


def getScanParameters(configPath):
    (
        apiURL,
        maxChunk,
        startBlock,
        endBlock,
        abi,
        contracts,
        saveInterval,
        fileString,
    ) = readConfig(configPath)
    w3, websocket = getW3(apiURL)
    eventNames, eventSigs, abiLookups, topicCounts = abiToSig(abi)
    if fileString == "":
        fileString = f"Block {startBlock} to {endBlock} events {','.join(eventNames)} contracts {','.join(contracts)}"
    if endBlock == "latest":
        endBlock = w3.eth.block_number
    return (
        w3,
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
        websocket,
    )


class EventScanner:
    def __init__(
        self, w3, contracts, eventSigs, abis, fileString, saveinterval, folder
    ):
        self.w3 = w3
        self.contracts = contracts
        # self.state = state
        self.eventSigs = eventSigs
        self.abis = abis
        self.abiLookups = {}
        self.topicCounts = {}
        self.latestBlock = 0
        self.saveInterval = saveinterval
        for x in range(len(abis)):
            self.abiLookups[eventSigs[x]] = abis[x]
            self.topicCounts[eventSigs[x]] = (
                sum(1 for inp in abis[x]["inputs"] if inp["indexed"]) + 1
            )
        self.fileString = folder + "output/" + fileString + ".json"
        self.backupFile = folder + "output/" + fileString + "bkup.json"
        try:
            with open(self.fileString, "r") as f:
                self.log = json.load(f)
                self.latestBlock = self.log["latest"]
                print(
                    f"existing file found: {self.fileString}, continuing from {self.log['latest']}"
                )

        except (json.JSONDecodeError, FileNotFoundError):
            try:
                with open(self.backupFile, "r") as f:
                    self.log = json.load(f)
                    self.latestBlock = self.log["latest"]
                    print(
                        f"backup file found: {self.backupFile}, continuing from {self.log['latest']}"
                    )
            except:
                print(f"no file found at: {self.fileString}, scanning from beginning")
                self.log = {"latest": 0}

    def scanChunk(self, start, end):
        allEvents = []
        filterParams = {
            "fromBlock": start,
            "toBlock": end,
            "topics": [self.eventSigs],
        }
        logs = self.w3.eth.get_logs(filterParams)
        for log in logs:
            if self.topicCounts[log["topics"][0].hex()] == len(log["topics"]):
                evt = get_event_data(
                    self.w3.codec, self.abiLookups[log["topics"][0].hex()], log
                )
                allEvents.append(evt)
        self.latestBlock = end
        return allEvents

    def getLastScannedBlock(self):
        return self.latestBlock

    def scan(self, start, end, maxChunk):
        print(
            f"starting scan at {time.asctime(time.localtime(time.time()))}, scanning to {end}"
        )
        if start < self.latestBlock:
            start = self.latestBlock
        startChunk = start
        endChunk = 0
        startTime = time.time()
        lastSave = startTime
        totalBlocks = end - start
        maxTries = 9
        tries = 0
        successes = 0
        currentChunk = int(maxChunk / 2)
        changeAmount = int((maxChunk) / 10)
        initialEventCount = len(self.log)
        numChunks = 0
        with tqdm(total=start - end) as progress_bar:
            while endChunk < end and (end - start > 0):
                try:
                    if end - startChunk > currentChunk:
                        endChunk = startChunk + currentChunk
                    else:
                        endChunk = end
                    events = self.scanChunk(startChunk, endChunk)
                    successes += 1
                    if successes > 5:
                        currentChunk += changeAmount
                    if currentChunk > maxChunk:
                        currentChunk = maxChunk
                    tries = 0
                    self.latestBlock = endChunk
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
                        f"Current block: {startChunk} , blocks in a scan batch: {currentChunk}, events processed in a batch {len(events)} ETA:{eta}"
                    )
                    progress_bar.update(currentChunk)
                    self.storeEventData(events)
                    startChunk = endChunk
                    numChunks += 1
                    if time.time() - lastSave > self.saveInterval:
                        self.saveState()
                except ValueError as e:
                    try:
                        if e.args[0]["message"] == "block range is too wide":
                            maxChunk = int(currentChunk * 0.98)
                            print(f"reduced max chunk size to {maxChunk}")
                    except:
                        pass
                    tries += 1
                    successes = 0
                    if tries > maxTries:
                        print("too many failures, try reducing maxChunk")
                        self.saveState()
                        sys.exit()
                    currentChunk -= changeAmount
                    print(e, f"\nreducing scan size to {currentChunk} blocks")
        print(
            f"Scanned total {len(self.log)-initialEventCount} events from {time.asctime(time.localtime(startTime))} to {time.asctime(time.localtime(time.time()))} in {numChunks} chunk"
        )
        self.saveState()

    def storeEventData(self, events):
        for param in events:
            blockNumber, txHash, address, index = getEventParameters(param)
            if blockNumber not in self.log:
                self.log[blockNumber] = {}
            if txHash not in self.log[blockNumber]:
                self.log[blockNumber][txHash] = {}
            if address not in self.log[blockNumber][txHash]:
                self.log[blockNumber][txHash][address] = {}
            self.log[blockNumber][txHash][address][index] = {}
            for eventName, eventParam in param["args"].items():
                self.log[blockNumber][txHash][address][index][eventName] = eventParam

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
        self.log["latest"] = self.latestBlock
        with open(self.fileString, "w") as f:
            f.write(json.dumps(self.log, indent=4))
        with open(self.backupFile, "w") as f:
            f.write(json.dumps(self.log, indent=4))

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
    with open(configPath) as f:
        cfg = json.load(f)
    apiURL = cfg["RPCSETTINGS"]["APIURL"]
    maxChunk = cfg["RPCSETTINGS"]["MAXCHUNKSIZE"]
    startBlock = cfg["SCANSETTINGS"]["STARTBLOCK"]
    endBlock = cfg["SCANSETTINGS"]["ENDBLOCK"]
    abi = cfg["SCANSETTINGS"]["ABI"]
    contracts = cfg["SCANSETTINGS"]["CONTRACTS"]
    fileString = cfg["FILESETTINGS"]["FILENAME"]
    saveinterval = cfg["FILESETTINGS"]["SAVEINTERVAL"]
    return (
        apiURL,
        maxChunk,
        startBlock,
        endBlock,
        abi,
        contracts,
        saveinterval,
        fileString,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()
    (
        w3,
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
        websocket,
    ) = getScanParameters(os.getenv("FOLDER") + "config.json")
    scan(
        w3,
        contracts,
        eventSigs,
        abi,
        fileString,
        saveInterval,
        startBlock,
        endBlock,
        maxChunk,
        os.getenv("FOLDER"),
    )

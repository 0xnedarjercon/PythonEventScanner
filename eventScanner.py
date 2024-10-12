from web3 import Web3
import time
import json
from web3._utils.events import get_event_data
from tqdm import tqdm
import os
from logger import Logger
import multiprocessing
from configLoader import loadConfig
from MultiWeb3 import MultiWeb3
from utils import blocks, toNative

directory = os.path.dirname(os.path.abspath(__file__))
from fileHandler import FileHandler
from functools import partial
from utils import decodeEvents


def scan():
    es = EventScanner()
    es.scanBlocks()


def scanLive():
    es = EventScanner()

    try:
        es.scan()
    except KeyboardInterrupt:
        print("keyboard interrupt detected, saving...")
        es.interrupt()
    return es.fileHandler.latest


def processEvents(event):
    eventName = event["name"]
    inputTypes = [input_abi["type"] for input_abi in event["inputs"]]
    eventSig = Web3.keccak(text=f"{eventName}({','.join(inputTypes)})").hex()
    topicCount = sum(1 for inp in event["inputs"] if inp["indexed"]) + 1
    return eventSig, topicCount


class InvalidConfigException(Exception):
    def __init__(self, text=""):
        self.text = text


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


class EventScanner(Logger):

    def __init__(
        self,
        configPath,
        showprogress=True,
        manager=None,
        jobManager=None,
        sharedResult=None,
        mw3=None,
    ):
        fileSettings, scanSettings, rpcSettings,web3Settings = loadConfig(configPath + "/config.json")
        self.live = False
        self.initMw3(sharedResult, jobManager, mw3, manager, configPath, web3Settings, rpcSettings)
        self.configPath = configPath
        Logger.setProcessName("scanner")
        self.loadSettings(scanSettings)
        super().__init__("es")
        self.fileHandler = FileHandler(fileSettings, configPath, "fh")
        self.showProgress = showprogress
        self.codec = self.mw3.codec

    # initialised a multiweb3 instance if one is not passed to it as well as a cross-process sharable results array
    # and job manager
    def initMw3(self, sharedResult, jobManager, mw3, manager, configPath, web3Settings,rpcSettings):
        if mw3 == None:
            if manager == None:
                self.manager = multiprocessing.Manager()
            else:
                self.manager = manager
            if sharedResult == None:
                self.results = MultiWeb3.createSharedResult(self.manager)
            else:
                self.results = sharedResult
            if jobManager == None:
                self.jobManager = MultiWeb3.createJobManager(self.manager)
            else:
                self.jobManger = jobManager
            self.mw3 = MultiWeb3(
                web3Settings,
                rpcSettings,
                manager=self.manager,
                configPath=configPath,
                results=self.results,
                jobManager=self.jobManager,
            )
        else:
            self.mw3 = mw3
            self.manager = mw3.manager
            self.results = mw3.results
            self.jobManager = mw3.MultiWeb3

        # stores relevent settings from config file

    def loadSettings(self, scanSettings):
        self.scanMode = scanSettings["MODE"]
        self.events = scanSettings["EVENTS"]
        self.loadAbis()
        self.processContracts(scanSettings["CONTRACTS"])
        self.processAbis()
        if scanSettings["STARTBLOCK"] == "current":
            self.startBlock = self.getCurrentBlock()
        else:
            self.startBlock = scanSettings["STARTBLOCK"]
        if scanSettings["ENDBLOCK"] == "current":
            self.endBlock = self.getCurrentBlock()
        else:
            self.endBlock = scanSettings["ENDBLOCK"]
        self.liveThreshold = scanSettings["LIVETHRESHOLD"]

    # breaks down event data into a usable dict
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

    # decodes events based on scansettings
    def decodeEvents(self, events):
        decodedEvents = []
        if self.scanMode == "ANYEVENT":
            for event in events:
                evt = get_event_data(
                    self.codec,
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
                        self.codec,
                        self.abiLookups[event["topics"][0].hex()][numTopics],
                        event,
                    )
                    decodedEvents.append(evt)
        return self.getEventData(decodedEvents)

    # processes the passed contracts and stores the event signiatures
    def processContracts(self, contracts):
        self.contracts = {}
        self.abiLookups = {}
        for contract, abiFile in contracts.items():
            checksumAddress = Web3.to_checksum_address(contract)
            self.contracts[checksumAddress] = {}
            for entry in self.abis[abiFile]:
                if entry["type"] == "event":
                    eventSig, topicCount = processEvents(entry)
                    self.contracts[checksumAddress][eventSig] = entry

    def processAbis(self):
        for abiFile, abi in self.abis.items():
            for entry in abi:
                if entry["type"] == "event" and entry["name"] in self.events:
                    eventSig, topicCount = processEvents(entry)
                    self.abiLookups[eventSig] = {topicCount: entry}

    # gets the lates block available from rpc
    def getCurrentBlock(self):
        return self.mw3.eth.get_block_number()

    def loadAbis(self):
        self.abis = {}
        files = os.listdir(self.configPath + "ABIs/")
        for file in files:
            if file.endswith(".json"):
                self.abis[file[:-5]] = json.load(open(self.configPath + "ABIs/" + file))

    # returns the last block stored by the filehandler
    def getLastStoredBlock(self):
        return self.fileHandler.latest

    # saves the currently available data
    def saveState(self):
        self.fileHandler.save()

    # graceful exit on keyboard interrupt
    def interrupt(self):
        self.logInfo("keyboard interrupt")
        self.saveState()

    # generates a filter for get_logs based on what is configured
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

    # scans a fixed range of blocks
    def scanFixedEnd(self, start, endBlock, callback=None):
        filterParams = self.getFilter(start, endBlock)
        if callback != None:
            decoded = True
        else:
            decoded = False
        cyclicGetLogs = self.mw3.setup_get_logs(filterParams, callback=callback).cyclic
        startTime = time.time()
        totalBlocks = endBlock - start
        self.logInfo(
            f"starting fixed scan at {time.asctime(time.localtime(startTime))}, scanning {start} to {endBlock}",
            True,
        )
        with tqdm(total=endBlock - start, disable=self.showProgress) as progress_bar:
            while self.fileHandler.latest < endBlock:
                cyclicGetLogs(callback=callback)
                scanResults = self.mw3.results.get()
                storedData = []
                if len(scanResults) > 0:
                    numBlocks = self.storeResults(
                        scanResults, decoded=decoded, endSourceFilter=True
                    )
                    # self.updateProgress(
                    #     progress_bar,
                    #     startTime,
                    #     start,
                    #     totalBlocks,
                    #     numBlocks,
                    # )
        self.logInfo(
            f"Completed: Scanned blocks {start}-{self.endBlock} in {time.time()-startTime}s from {time.asctime(time.localtime(startTime))} to {time.asctime(time.localtime(time.time()))}",
            True,
        )
        self.logInfo(
            f"average {(endBlock-start)/(time.time()-startTime)} blocks per second",
            True,
        )
        self.fileHandler.save()
        return endBlock
        # updates progress bar for fixed scan

    def updateProgress(
        self,
        progress_bar,
        startTime,
        start,
        totalBlocks,
        numBlocks,
    ):
        elapsedTime = time.time() - startTime
        progress = self.fileHandler.latest - start
        avg = progress / elapsedTime + 0.1
        remainingTime = (totalBlocks - progress) / avg
        eta = f"{int(remainingTime)}s ({time.asctime(time.localtime(time.time()+remainingTime))})"
        progress_bar.set_description(
            f"Stored up to: {self.fileHandler.latest} ETA:{eta} avg: {avg} blocks/s {progress}/{totalBlocks}"
        )
        progress_bar.update(numBlocks)

    # stores get_logs results into the file handler
    def storeResults(
        self, scanResults, forceSave=False, decoded=False, endSourceFilter=False
    ):
        storedData = []
        if len(scanResults) > 0:
            for scanResult in scanResults:
                if not decoded:
                    decodedEvents = self.decodeEvents(scanResult[-1])

                else:
                    decodedEvents = scanResult[-1]
                self.logInfo(
                    f"events found: {len(decodedEvents)}  {list(decodedEvents.keys())[0]} {list(decodedEvents.keys())[-1]}"
                )
                blockNums = list(decodedEvents.keys())
                i = 0
                blockNum = blockNums[i]
                while blockNum < self.fileHandler.latest:
                    if len(decodedEvents) > 0:
                        return
                    del decodedEvents[blockNum]
                    i += 1
                    blockNum = blockNums[i]
                    # newEvents = {x:y for x, y in decodedEvents[-1].items() if int(x)> self.fileHandler.latest}
                if endSourceFilter:
                    end = scanResult[1][0]["toBlock"]
                else:
                    end = list(decodedEvents.keys())[-1]

                storedData.append(
                    [
                        max(self.fileHandler.latest, scanResult[1][0]["fromBlock"]),
                        decodedEvents,
                        end,
                    ]
                )
                self.fileHandler.process(
                    [
                        [
                            max(self.fileHandler.latest, scanResult[1][0]["fromBlock"]),
                            decodedEvents,
                            end,
                        ]
                    ]
                )
            if forceSave:
                self.fileHandler.save()
            return self.fileHandler.process(storedData)
        else:
            return 0

    # scans from a specified block, then transitions to live mode, polling for latest blocks
    def scanBlocks(self, start=None, end=None, resultsOut=None, rpcs=None, decode=True):
        if start is None:
            start = self.startBlock
        if end is None:
            end = self.endBlock
        if resultsOut is None:
            resultsOut = self.mw3.results
        if end == "current":
            end = self.getCurrentBlock()
        if decode:
            callback = partial(
                decodeEvents,
                scanMode=self.scanMode,
                codec=self.codec,
                contracts=self.contracts,
                abiLookups=self.abiLookups,
            )
        else:
            callback = None
        if isinstance(end, int):
            self.scanMissingBlocks(start, end, callback=callback)
            return
        else:
            self.fileHandler.setup(start)
            _end = self.getCurrentBlock()
            self.logInfo(f"latest block {_end}, latest stored {end}")
            while _end - self.fileHandler.latest > self.liveThreshold:
                _end = self.getCurrentBlock()
                self.scanMissingBlocks(start, _end, callback=callback)
            self.logInfo(
                f"------------------going into live mode, current block: {_end} latest: {self.fileHandler.latest}------------------",
                True,
            )
            self.fileHandler.setup(start)
            filterParams = self.getFilter(self.fileHandler.latest + 1, end)

            self.mw3.setup_get_logs(filterParams, self.results)
            self.mw3.mGet_logsLatest(callback=callback)
            self.live = True

    # triggers fixed scan for any gaps in the stored data for the range provided
    def scanMissingBlocks(self, start, end, callback=None):
        missingBlocks = self.fileHandler.checkMissing(start, end)
        currentBlock = self.getCurrentBlock()
        self.logInfo(f"missing blocks: {missingBlocks}")
        for missingBlock in missingBlocks:
            assert missingBlock[0] <= currentBlock, 'startBlock not yet available'
            self.fileHandler.setup(missingBlock[0])
            self.scanFixedEnd(missingBlock[0], missingBlock[1], callback=callback)
        self.fileHandler.setup(end)

    def getEvents(self, start, end, results={}):
        self.scanMissingBlocks(start, end)
        self.fileHandler.getEvents(start, end, results)
        return results


def readConfig(configPath):
    with open(configPath + "config.json") as f:
        cfg = json.load(f)
    return cfg["RPCSETTINGS"], cfg["SCANSETTINGS"], cfg["FILESETTINGS"]


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    folderPath = os.getenv("FOLDER_PATH")
    _configPath = f"{os.path.dirname(os.path.abspath(__file__))}/settings/{folderPath}/"
    with open(_configPath + "/config.json") as f:
        cfg = json.load(f)
    fileSettings, scanSettings, rpcSettings, web3Settings = loadConfig(cfg)
    es = EventScanner(
        _configPath,
    )
    es.scanBlocks(decode=True)
    while not es.live:
        time.sleep(1)
    while es.live:
        results = es.results.get()
        # do stuff with the data here
        print(len(results))
        if (len(results)) > 0:
            es.storeResults(results, decoded=True, forceSave=True)
        print(es.fileHandler.latest)
        print(es.getCurrentBlock())

        time.sleep(1)

# <multiprocessingUtils.SharedResult object at 0x7f5c60b51120>
# <multiprocessingUtils.SharedResult object at 0x7f5c60b51120>

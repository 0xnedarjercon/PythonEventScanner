from web3 import Web3
import time
import json
from web3._utils.events import get_event_data
from tqdm import tqdm
import os
from logger import Logger
import multiprocessing
from configLoader import loadConfig
import atexit
from MultiWeb3 import MultiWeb3
from utils import blocks, toNative
directory = os.path.dirname(os.path.abspath(__file__))
from rpc import RPC
from fileHandler import FileHandler


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
        manager = None,
        jobManager = None,
        sharedResult = None,
        mw3 = None
    ):

        fileSettings, scanSettings, rpcSettings, rpcInterfaceSettings, hreSettings, web3Settings = (
            loadConfig(_configPath + "/config.json")
    )

        self.initMw3(sharedResult, jobManager, mw3, manager, configPath, scanSettings)
        atexit.register(self.teardown)
        self.configPath = configPath
        Logger.setProcessName(scanSettings["NAME"])
        super().__init__(configPath, scanSettings, 'es')
        self.loadSettings(scanSettings, rpcSettings, rpcInterfaceSettings)
        self.fileHandler = FileHandler(
            fileSettings,
            configPath, 'fh'
        )
        self.showProgress = showprogress
        self.validateSettings()
        self.codec = self.mw3.codec
        
    def initMw3(self, sharedResult, jobManager, mw3, manager, configPath, scanSettings):
        if mw3 == None:
            if manager == None:
                self.manager = multiprocessing.Manager()
            else:
                self.manager = manager
            if sharedResult == None:
                self.results = MultiWeb3.createSharedResult(self.manager, configPath,scanSettings, name = 'sr' )
            else:
                self.results = sharedResult
            if jobManager == None:
                self.jobManager = MultiWeb3.createJobManager(self.manager, configPath,scanSettings, 'jm')
            else:
                self.jobManger = jobManager  
            self.mw3 = MultiWeb3(web3Settings, rpcSettings, manager = self.manager, configPath=configPath, results = self.results, jobManager = self.jobManager)
        else:
            self.mw3 = mw3
            self.manager = mw3.manager
            self.results = mw3.results
            self.jobManager = mw3.jobManager
            
            
            
    def validateSettings(self):
        if len(self.abiLookups) == 0 and self.scanMode == "ANYCONTRACT":
            raise InvalidConfigException(
                "invalid configuration, unable to find any abis"
            )
        if len(self.contracts) == 0 and self.scanMode == "ANYEVENT":
            raise InvalidConfigException(
                "invalid configuration, unable to find any contracts"
            )
            
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
    
    def loadSettings(self, scanSettings, rpcSettings, rpcInterfaceSettings):
        self.scanMode = scanSettings["MODE"]
        self.events = scanSettings["EVENTS"]
        self.loadAbis()
        self.processContracts(scanSettings["CONTRACTS"])
        # self.initRpcs(rpcSettings, scanSettings, rpcInterfaceSettings)
        if scanSettings["STARTBLOCK"] == "current":
            self.startBlock = self.getCurrentBlock()
        else:
            self.startBlock = scanSettings["STARTBLOCK"]
        if scanSettings["ENDBLOCK"] == "current":
            self.endBlock = self.getCurrentBlock()
        else:
            self.endBlock = scanSettings["ENDBLOCK"]
        self.liveThreshold = scanSettings["LIVETHRESHOLD"]

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
                    if entry["name"] in self.events:
                        self.abiLookups[eventSig] = {topicCount: entry}

    def getCurrentBlock(self):
        return self.mw3.eth.get_block_number()

    def loadAbis(self):
        self.abis = {}
        files = os.listdir(self.configPath + "ABIs/")
        for file in files:
            if file.endswith(".json"):
                self.abis[file[:-5]] = json.load(open(self.configPath + "ABIs/" + file))

    def getLastStoredBlock(self):
        return self.fileHandler.latest

    def saveState(self):
        self.fileHandler.save()

    def interrupt(self):
        self.logInfo("keyboard interrupt")
        self.teardown()

    def teardown(self):
        self.fileHandler.save()


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
    def scanFixedEnd(self, start, endBlock):
        filterParams = self.getFilter(start, endBlock)
        cyclicGetLogs = self.mw3.setup_get_logs(filterParams).cyclic
        startTime = time.time()
        totalBlocks = endBlock - start
        self.logInfo(
            f"starting fixed scan at {time.asctime(time.localtime(startTime))}, scanning {start} to {endBlock}",
            True,
        )
        with tqdm(total=endBlock - start, disable=self.showProgress) as progress_bar:
            while self.fileHandler.latest < endBlock:
                cyclicGetLogs()
                scanResults = self.mw3.results.get()
                storedData = []
                if len(scanResults)>0:
                    for scanResult in scanResults:
                        decodedEvents = self.decodeEvents(scanResult[-1])
                        self.logInfo(f"events found: {len(decodedEvents)}  {blocks(scanResult)}")
                        storedData.append([scanResult[1][0]['fromBlock'], decodedEvents, scanResult[1][0]['toBlock']])
                    numBlocks = self.fileHandler.process(storedData)
                    self.updateProgress(
                        progress_bar,
                        startTime,
                        start,
                        totalBlocks,
                        numBlocks,
                    )
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

    def scanBlocks(self, start=None, end=None, resultsOut=None, rpcs = None, continuous = False):
        if start is None:
            start = self.startBlock
        if end is None:
            end = self.endBlock
        if resultsOut is None:
            resultsOut = self.mw3.results
        if end == "current":
            end = self.getCurrentBlock()
        if isinstance(end, int):
            self.scanMissingBlocks(start, end)
            return
        else:
            while True:
                _end = self.getCurrentBlock()
                self.scanMissingBlocks(start, _end)
                if self.fileHandler.latest >= _end - self.liveThreshold:
                    filterParams = self.getFilter(self.fileHandler.latest , end)
                    cyclicGetLogs=self.mw3.setup_get_logs(filterParams).cyclic
                    while True:
                        cyclicGetLogs()
                        # scanResults = self.results.get()
                        time.sleep(0.001)


    def scanMissingBlocks(self, start, end):
        missingBlocks = self.fileHandler.checkMissing(start, end)
        self.logInfo(f"missing blocks: {missingBlocks}")
        for missingBlock in missingBlocks:
            self.fileHandler.setup(missingBlock[0])
            self.scanFixedEnd(missingBlock[0], missingBlock[1])
        self.fileHandler.setup(end)

    def getEvents(self, start, end, results=[]):
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
    fileSettings, scanSettings, rpcSettings, rpcInterfaceSettings, hreSettings, web3Settings = (
        loadConfig(cfg)
    )
    es = EventScanner(
        _configPath,
    )
    es.scanBlocks()

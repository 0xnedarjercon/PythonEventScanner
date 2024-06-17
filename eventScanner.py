from web3 import Web3
import time
import json
from tqdm import tqdm
import os
from logger import Logger
import multiprocessing
from configLoader import loadConfig
import atexit
from scannerRpcInterface import initInterfaces

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


class EventScanner(Logger):

    def __init__(
        self,
        fileSettings,
        scanSettings,
        rpcSettings,
        rpcInterfaceSettings,
        configPath,
        showprogress=True,
    ):

        atexit.register(self.teardown)
        self.configPath = configPath
        Logger.setProcessName(scanSettings["NAME"])
        super().__init__(configPath, scanSettings)
        self.loadSettings(scanSettings, rpcSettings, rpcInterfaceSettings)
        self.fileHandler = FileHandler(
            fileSettings,
            configPath,
        )
        self.showProgress = showprogress
        self.validateSettings()

    def validateSettings(self):

        if len(self.abiLookups) == 0 and self.scanMode == "ANYCONTRACT":
            raise InvalidConfigException(
                "invalid configuration, unable to find any abis"
            )
        if len(self.contracts) == 0 and self.scanMode == "ANYEVENT":
            raise InvalidConfigException(
                "invalid configuration, unable to find any contracts"
            )

    def loadSettings(self, scanSettings, rpcSettings, rpcInterfaceSettings):
        self.scanMode = scanSettings["MODE"]
        self.events = scanSettings["EVENTS"]
        self.loadAbis()
        self.processContracts(scanSettings["CONTRACTS"])
        self.initRpcs(rpcSettings, scanSettings, rpcInterfaceSettings)
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
        if self.rpc:
            return self.rpc.w3.eth.get_block_number()
        else:
            block = self.iJobManager.addJob("w3.eth.get_block_number")
            self.logInfo(f"current block is {block}")
            return block

    def loadAbis(self):
        self.abis = {}
        files = os.listdir(self.configPath + "ABIs/")
        for file in files:
            if file.endswith(".json"):
                self.abis[file[:-5]] = json.load(open(self.configPath + "ABIs/" + file))

    def initRpcs(self, rpcSettings, _scanSettings, rpcInterfaceSettings):
        if len(rpcSettings) == 0:
            raise InvalidConfigException("need at least one RPC configured")
        elif len(rpcSettings) > 1:
            multiprocessing = True
        else:
            multiprocessing = False

        self.iFixedScan, self.iLiveScan, self.iJobManager = initInterfaces(
            self.configPath, rpcInterfaceSettings, multiprocessing
        )
        self.processes = []
        self.rpc = None

        if _scanSettings["RPC"]:
            scannerRpc = rpcSettings.pop(0)
            scannerRpc["NAME"] = _scanSettings["NAME"]
            self.rpc = RPC(
                scannerRpc,
                self.scanMode,
                self.contracts,
                self.abiLookups,
                self.iFixedScan,
                self.iJobManager,
                self.iLiveScan,
                self.configPath,
            )
        for rpcSetting in rpcSettings:
            process = multiprocessing.Process(
                target=self.initRPCProcess,
                args=(rpcSetting,),
            )
            self.processes.append(process)
            process.start()

    def getLastStoredBlock(self):
        return self.fileHandler.latest

    def saveState(self):
        self.fileHandler.save()

    def interrupt(self):
        self.logInfo("keyboard interrupt")
        self.teardown()

    def teardown(self):
        self.iJobManager.state = -1
        self.fileHandler.save()

    def initRPCProcess(
        self,
        settings,
    ):
        rpc = RPC(
            settings,
            self.scanMode,
            self.contracts,
            self.abiLookups,
            self.iFixedScan,
            self.iJobManager,
            self.iLiveScan,
            self.configPath,
        )
        try:
            rpc.run()
        except Exception as e:
            self.logCritical(f"process failed {e}")

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

    def scanFixedEnd(self, start, endBlock):
        startTime = time.time()
        totalBlocks = endBlock - start
        self.iFixedScan.addScanRange(start, endBlock)
        self.iJobManager.state = 1
        self.logInfo(
            f"starting fixed scan at {time.asctime(time.localtime(startTime))}, scanning {start} to {endBlock}",
            True,
        )
        with tqdm(total=endBlock - start, disable=self.showProgress) as progress_bar:
            while self.fileHandler.latest < endBlock:
                numBlocks = 0
                if self.rpc:
                    self.rpc.fixedScan()
                    results = self.iFixedScan.checkResults()
                else:
                    results = self.iFixedScan.waitResults()
                numBlocks = self.fileHandler.process(results)
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

    def scanBlocks(
        self, start=None, end=None, resultsOut=None, callback=None, storeResults=True
    ):
        if start is None:
            start = self.startBlock
        if end is None:
            end = self.endBlock
        if resultsOut is None:
            resultsOut = []
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
                    self.scanLive(resultsOut, callback, storeResults)

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

    def scanLive(
        self,
        resultsOut=None,
        callback=None,
        storeResults=True,
    ):
        self.iJobManager.state = 2
        startBlock = self.iLiveScan.last = self.getLastStoredBlock()
        self.logInfo(f"livescanning from block {startBlock}", True)
        if resultsOut == None:
            resultsOut = []
        while True:
            if self.rpc:
                self.rpc.scanLive()
                results = self.iLiveScan.checkResults()
            else:
                results = self.iLiveScan.waitResults()
            resultsOut += results
            if callback != None:
                callback(resultsOut)
            if storeResults and resultsOut:
                self.fileHandler.process(resultsOut)
            resultsOut.clear()


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
    fileSettings, scanSettings, rpcSettings, rpcInterfaceSettings, hreSettings = (
        loadConfig(cfg)
    )
    es = EventScanner(
        fileSettings,
        scanSettings,
        rpcSettings,
        rpcInterfaceSettings,
        _configPath,
    )
    es.scanBlocks()

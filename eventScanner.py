from web3 import Web3
import time
import threading
import json
from tqdm import tqdm
import os
from logger import Logger, startListener
import multiprocessing
from configLoader import scanSettings, rpcSettings, configPath, rpcInterfaceSettings
from scannerRpcInterface import ScannerRPCInterface
import atexit


directory = os.path.dirname(os.path.abspath(__file__))
from rpc import RPC
from fileHandler import FileHandler


def scan():
    es = EventScanner()
    es.scan()


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


class EventScanner(Logger):

    def __init__(self):
        atexit.register(self.teardown)
        startListener()
        self.iRpc = ScannerRPCInterface(rpcInterfaceSettings)
        self.threads = []
        super().__init__("scanner")
        self.loadSettings(scanSettings, rpcSettings)
        self.fileHandler = FileHandler(self.startBlock)

    def loadSettings(self, scanSettings, rpcSettings):
        super().__init__("scanner", scanSettings["DEBUGLEVEL"])
        self.scanMode = scanSettings["MODE"]
        self.events = scanSettings["EVENTS"]
        self.loadAbis()
        self.processContracts(scanSettings["CONTRACTS"])
        self.initRpcs(rpcSettings)
        if scanSettings["STARTBLOCK"] == "current":
            self.startBlock = self.iRpc.syncRequest("w3.eth.get_block_number")
        else:
            self.startBlock = scanSettings["STARTBLOCK"]
        if scanSettings["ENDBLOCK"] == "current":
            self.endBlock = self.iRpc.syncRequest("w3.eth.get_block_number")
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

    def loadAbis(self):
        self.abis = {}
        files = os.listdir(configPath + "ABIs/")
        for file in files:
            if file.endswith(".json"):
                self.abis[file[:-5]] = json.load(open(configPath + "ABIs/" + file))

    def initRpcs(self, rpcSettings):
        self.processes = []
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
        self.iRpc.state = -1
        self.fileHandler.save()

    def initRPCProcess(self, settings):
        rpc = RPC(
            settings,
            self.iRpc,
            self.scanMode,
            self.contracts,
            self.abiLookups,
        )
        try:
            rpc.run()
        except Exception as e:
            self.logCritical(f"process failed {e}")

    def checkRpcJobs(self, currentBlock, endBlock, maxChunkSize):
        numChunks = 0
        for rpc in self.rpcs:
            if len(rpc.jobs) == 0:
                if len(rpc.currentResults) > 0:
                    results, start, end = rpc.getResults()
                    self.logInfo(
                        f"data added to pending {start} to {end} from {rpc.apiUrl}"
                    )
                    self.fileHandler.process(results, start, end)
                    rpc.currentResults.clear()
                if endBlock > currentBlock:
                    numChunks += 1
                    currentBlock = rpc.addJob(
                        currentBlock, min([endBlock, currentBlock + maxChunkSize])
                    )
        return currentBlock, numChunks

    def updateProgress(
        self,
        progress_bar,
        startTime,
        currentBlock,
        start,
        totalBlocks,
        numBlocks,
        remainingTime,
    ):
        elapsedTime = time.time() - startTime
        avg = (self.fileHandler.latest - start) / elapsedTime + 0.1
        remainingTime = (totalBlocks - (self.fileHandler.latest - start)) / avg
        eta = f"{int(remainingTime)}s ({time.asctime(time.localtime(time.time()+remainingTime))})"
        progress_bar.set_description(
            f"Current block: {currentBlock} stored up to: {self.fileHandler.latest} ETA:{eta} avg: {avg} blocks/s"
        )
        progress_bar.update(numBlocks)

    def scanFixedEnd(self, start, endBlock):
        startTime = time.time()
        totalBlocks = endBlock - start
        currentBlock = start
        remainingTime = 1
        self.iRpc.setScanRange(start, endBlock)
        self.iRpc.state = 1
        self.logInfo(
            f"starting fixed scan at {time.asctime(time.localtime(startTime))}, scanning {start} to {endBlock}",
            True,
        )
        with tqdm(total=endBlock - start) as progress_bar:
            while self.fileHandler.latest < endBlock:
                prevStart = start
                results = self.iRpc.readScanResults()
                for result in results:
                    numBlocks = self.fileHandler.process(result)
                    currentBlock = result[2]
                self.updateProgress(
                    progress_bar,
                    startTime,
                    currentBlock,
                    start,
                    totalBlocks,
                    numBlocks,
                    remainingTime,
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

    def scan(self, storeResults=False, callback=None, resultsOut=None):
        try:
            start = max([self.startBlock, self.fileHandler.latest])
            if type(self.endBlock) == int:
                self.scanFixedEnd(start, self.endBlock)
            elif self.endBlock == "latest":
                latestBlock = self.iRpc.syncRequest("w3.eth.get_block_number")
                while latestBlock > self.fileHandler.latest + self.liveThreshold:
                    self.logInfo(f"scanning to latest block: {latestBlock}")
                    self.scanFixedEnd(start, latestBlock)
                    latestBlock = self.iRpc.syncRequest("w3.eth.get_block_number")
                self.logInfo(
                    f"close enough to latest block: {self.fileHandler.latest} - {latestBlock}. starting live mode"
                )
                self.scanLive(
                    resultsOut,
                    callback,
                    storeResults,
                )
            self.teardown()

        except KeyboardInterrupt:
            print("keyboard interrupt detected, saving...")
            self.interrupt()
        return self.fileHandler.latest

    def scanLive(
        self,
        resultsOut=None,
        callback=None,
        storeResults=True,
    ):
        if callback == None and resultsOut == None and not storeResults:
            self.logWarn("data not being stored or used during livescan", True)
        self.iRpc.state = 2
        self.iRpc.start = self.getLastStoredBlock()
        if resultsOut == None:
            resultsOut = {}
        while True:
            resultsOut.update(self.iRpc.getLiveResults())
            if callback != None:
                callback(resultsOut)
            if storeResults:
                self.fileHandler.process(resultsOut)
            resultsOut.clear()


def readConfig(configPath):
    with open(configPath + "config.json") as f:
        cfg = json.load(f)
    return cfg["RPCSETTINGS"], cfg["SCANSETTINGS"], cfg["FILESETTINGS"]


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    scan()

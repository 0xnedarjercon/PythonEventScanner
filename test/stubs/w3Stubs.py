import json
import os
from web3 import Web3
import time

from rpc import RPC
from eventScanner import EventScanner, InvalidConfigException
import multiprocessing
import pickle
import asyncio
from logger import Logger
from collections import deque


def gatherData(folder):
    data = {}
    for file in os.listdir(folder):
        with open(folder + file, "r") as f:
            data.update(json.load(f))
    return data


class StubbedEth:
    def __init__(self, cfg):
        self.data = {}
        self.startBlock = cfg["STUBSTARTBLOCK"]
        self.maxBlocks = cfg["STUBMAXBLOCKS"]
        self.maxEvents = cfg["STUBMAXEVENTS"]
        self.loadData()
        self.lastBlock = int(list(self.data.keys())[-1])
        self.startTime = time.time()
        self.ended = False
        self.mining = False
        self.errorCount = 0

    @property
    def currentBlock(self):
        if self.mining:
            return int(
                self.startBlock
                + (time.time() - self.startTime) * (self.lastBlock - self.startBlock)
            )
        else:
            return self.startBlock

    def get_block_number(self):
        return self.currentBlock

    def loadData(self):
        with open("./test/tmp/comparisonData/testData.pkl", "rb") as f:
            logs = pickle.load(f)
        for log in logs:
            if log.blockNumber not in self.data:
                self.data[log.blockNumber] = []
            self.data[log.blockNumber].append(log)
        print(list(self.data.keys())[-1])

    def get_logs(self, filterParams):
        _from = filterParams["fromBlock"]
        to = min(filterParams["toBlock"], self.currentBlock)
        to = filterParams["toBlock"]
        results = []

        if _from >= self.startBlock:
            self.mining = True
        if self.ended:
            raise KeyboardInterrupt
        if _from - to > self.maxBlocks:
            raise ValueError({"message": "block range is too wide"})
        if _from >= self.lastBlock:
            self.ended = True
        for i in range(_from, to + 1):
            if i in self.data:
                results += self.data[i]
        if len(results) > self.maxEvents:
            raise asyncio.exceptions.TimeoutError
        return results


class StubbedW3(Web3):
    def __init__(self, cfg):
        self.eth = StubbedEth(cfg)
        with open("test/tmp/codec", "rb") as f:
            self.codec = pickle.load(f)


class StubbedRpc(RPC):

    def __init__(
        self,
        rpcSettings,
        scanMode,
        contracts,
        abiLookups,
        iFixedScan,
        iJobManager,
        iLiveScan,
        configPath,
    ):
        self.apiUrl = rpcSettings["APIURL"]
        self.iFixedScan = iFixedScan
        self.iJobManager = iJobManager
        self.iLiveScan = iLiveScan
        Logger.setProcessName(rpcSettings["NAME"])
        super(RPC, self).__init__(configPath, rpcSettings)
        self.isHH = False
        if type(rpcSettings["APIURL"]) == dict:
            self.initHREW3(configPath, rpcSettings["APIURL"])
        else:
            self.w3 = StubbedW3(rpcSettings)
        self.maxChunkSize = rpcSettings["MAXCHUNKSIZE"]
        self.currentChunkSize = rpcSettings["STARTCHUNKSIZE"]
        self.eventsTarget = rpcSettings["EVENTSTARGET"]
        self.pollInterval = rpcSettings["POLLINTERVAL"]
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
        self.completedJobs = deque(maxlen=20)
        self.evt = []
        self.websocket = False


class StubbedEventScanner(EventScanner):
    def initRpcs(self, rpcSettings, scanSettings, rpcInterfaceSettings):
        if len(rpcSettings) > 1:
            mp = True
        else:
            mp = False
        if len(rpcSettings) == 0:
            raise InvalidConfigException("need at least one RPC configured")
        self.iFixedScan, self.iLiveScan, self.iJobManager = initInterfaces(
            self.configPath, rpcInterfaceSettings, mp
        )
        self.processes = []
        self.rpc = None

        if scanSettings["RPC"]:
            scannerRpc = rpcSettings.pop(0)
            scannerRpc["NAME"] = scanSettings["NAME"]
            self.rpc = StubbedRpc(
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

    def latestStored(self):
        files = self.fileHandler.getFiles()
        if files:
            return files[-1][-1]
        else:
            return 0

    def initRPCProcess(self, settings):
        rpc = StubbedRpc(
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

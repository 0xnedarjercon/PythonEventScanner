from web3 import Web3
import time
from web3._utils.events import get_event_data
import threading
import json
from tqdm import tqdm
import os
import logging


directory = os.path.dirname(os.path.abspath(__file__))


from rpc import RPC
from fileHandler import FileHandler


def scan():
    es = EventScanner()
    try:
        es.scan()
    except KeyboardInterrupt:
        print("keyboard interrupt detected, saving...")
        es.teardown()
    return es.fileHandler.latest


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
        self.threads = []
        self.configPath = f"{directory}/settings/{folderPath}/"
        rpcSettings, scanSettings, fileSettings = readConfig(self.configPath)
        
        self.loadScanSettings(scanSettings)
        self.loadFileSettings(fileSettings)
        self.loadRPCSettings(rpcSettings)
        

    def loadScanSettings(self, scanSettings):
        self.debug = scanSettings["DEBUG"]
        self.startBlock = scanSettings["STARTBLOCK"]
        if scanSettings["ENDBLOCK"] == "latest":
            self.endBlock = self.rpcs[0].w3.eth.get_block_number()
        else:
            self.endBlock = scanSettings["ENDBLOCK"]
        self.mode = scanSettings["MODE"]
        self.events = scanSettings["EVENTS"]
        self.contracts = processContracts(self.configPath, scanSettings["CONTRACTS"])
        self.abis = scanSettings["ABI"]
        self.forceNew = scanSettings["FORCENEW"]

    def loadRPCSettings(self, rpcSettings):
        self.rpcs = []
        for rpcSetting in rpcSettings:
            self.rpcs.append(RPC(rpcSetting, self))

    def loadFileSettings(self, fileSettings):
        filePath = self.configPath + self.getFileString(fileSettings)
        try:
            os.mkdir(filePath)
        except:
            pass
        self.log = logging.getLogger(__name__)
        logging.basicConfig(
            filename=f"{filePath}/debug.log",  # Logfile name
            filemode="a",  # Append mode
            format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
            level=logging.INFO,  # Log level
        )
        
        if self.debug:
            self.log.setLevel(logging.INFO)
        else:
            self.log.setLevel(logging.CRITICAL)
        self.fileHandler = FileHandler(
            filePath, self, fileSettings["MAXENTRIES"], fileSettings["SAVEINTERVAL"]
        )

        self.log.info("start new session")

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

    def getLastScannedBlock(self):
        return self.fileHandler.latest

    def saveState(self):
        self.fileHandler.save()

    def teardown(self):
        for rpc in self.rpcs:
            rpc.running = False
        self.saveState()

    def scan(self):
        startTime = time.time()
        start = max([self.startBlock, self.fileHandler.latest])
        startChunk = start
        endChunk = 0
        remainingTime = 0
        totalBlocks = self.endBlock - start
        numChunks = 0
        print(
            f"starting scan at {time.asctime(time.localtime(startTime))}, scanning {start} to {self.endBlock}"
        )

        for rpc in self.rpcs:
            thread = threading.Thread(target=rpc.run)
            self.threads.append(thread)
            thread.start()
        with tqdm(total=start - self.endBlock) as progress_bar:
            while self.fileHandler.latest < self.endBlock:
                prevStart = start
                for rpc in self.rpcs:
                    if len(rpc.jobs) == 0:
                        if len(rpc.currentResults) > 0:
                            self.fileHandler.log(rpc.currentResults, rpc.start, rpc.end)
                            rpc.currentResults = {}
                            endChunk = rpc.end
                        if self.endBlock > startChunk:
                            numChunks += 1
                            startChunk = rpc.addJob(startChunk, self.endBlock)
                    else:
                        pass
                elapsedTime = time.time() - startTime
                progress = (startChunk - start) / totalBlocks
                if progress > 0:
                    totalEstimatedTime = elapsedTime / progress
                    remainingTime = ((totalEstimatedTime * (1 - progress))+remainingTime*3)/4
                    
                    eta = f'{int(remainingTime)}s ({time.asctime(time.localtime(time.time()+remainingTime))})'
                else:
                    eta = "INF"
                progress_bar.set_description(f"Current block: {startChunk} ETA:{eta}")
                progress_bar.update(start - prevStart)
        print(
            f"Scanned blocks {start}-{self.endBlock} from {time.asctime(time.localtime(startTime))} to {time.asctime(time.localtime(time.time()))} in {numChunks} chunks"
        )
        print(
            f"average {(self.endBlock-start)/(time.time()-startTime)} blocks per second"
        )
        self.teardown()


def runRpc(rpc):
    rpc.run()


def readConfig(configPath):
    with open(configPath + "config.json") as f:
        cfg = json.load(f)
    return cfg["RPCSETTINGS"], cfg["SCANSETTINGS"], cfg["FILESETTINGS"]


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    scan()

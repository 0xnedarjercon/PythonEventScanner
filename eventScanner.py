from web3 import Web3
import time
from web3._utils.events import get_event_data
import threading
import json
from tqdm import tqdm
import os
from logger import Logger



directory = os.path.dirname(os.path.abspath(__file__))


from rpc import RPC
from fileHandler import FileHandler


def scan():
    es = EventScanner()
    try:
        es.scan()
    except KeyboardInterrupt:
        print("keyboard interrupt detected, saving...")
        es.interrupt()
    return es.fileHandler.latest


class EventScanner(Logger):

    def __init__(self):
        
        folderPath = os.getenv("FOLDER_PATH")
        self.threads = []
        self.configPath = f"{os.path.dirname(os.path.abspath(__file__))}/settings/{folderPath}/"
        rpcSettings, scanSettings, fileSettings = readConfig(self.configPath)
        self.loadScanSettings(scanSettings)
        self.fileHandler = FileHandler(self, fileSettings, scanSettings["DEBUGLEVEL"])
        self.loadRPCSettings(rpcSettings)
        
    def loadScanSettings(self, scanSettings):
        super().__init__('scanner', scanSettings["DEBUG"])
        self.startBlock = scanSettings["STARTBLOCK"]
        if scanSettings["ENDBLOCK"] == "current":
            self.endBlock = self.rpcs[0].w3.eth.get_block_number()
        else:
            self.endBlock = scanSettings["ENDBLOCK"]
        self.mode = scanSettings["MODE"]
        self.events = scanSettings["EVENTS"]
        self.loadAbis()
        self.processContracts(scanSettings["CONTRACTS"])
        self.forceNew = scanSettings["FORCENEW"]
        
    def processEvents(self, event):
        eventName = event["name"]
        inputTypes = [input_abi["type"] for input_abi in event["inputs"]]
        eventSig = Web3.keccak(text=f"{eventName}({','.join(inputTypes)})").hex()
        topicCount = sum(1 for inp in event["inputs"] if inp["indexed"]) + 1
        return eventSig, topicCount      
    
    def processContracts(self, contracts):
        self.contracts = {}
        self.abiLookups = {}
        for contract, abiFile in contracts.items():
            checksumAddress = Web3.to_checksum_address(contract)
            self.contracts[checksumAddress] = {}
            for entry in self.abis[abiFile]:
                if entry["type"] == "event":
                    eventSig, topicCount = self.processEvents(entry)
                    self.contracts[checksumAddress][eventSig] = entry
                    if entry['name'] in self.events:
                        self.abiLookups[eventSig]={topicCount: entry}

    
    def loadAbis(self):
        self.abis = {}
        files = os.listdir(self.configPath+ 'ABIs/')
        for file in files:
            if file.endswith('.json'):
                self.abis[file[:-5]]=json.load(open(self.configPath+ 'ABIs/'+file))
                
    def loadRPCSettings(self, rpcSettings):
        self.rpcs = []
        for rpcSetting in rpcSettings:
            self.rpcs.append(RPC(rpcSetting, self))


        



    def getLastScannedBlock(self):
        return self.fileHandler.latest

    def saveState(self):
        self.fileHandler.save()
    def interrupt(self):
        self.logInfo('keyboard interrupt')
        self.teardown()
    def teardown(self):
        for rpc in self.rpcs:
            rpc.running = False
        self.fileHandler.save()

    def scan(self):
        startTime = time.time()
        start = max([self.startBlock, self.fileHandler.latest])
        startChunk = start
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
                            results, start, end = rpc.getResults()
                            self.logInfo(f"data added to pending {start} to {end} from {rpc.apiUrl}")
                            self.fileHandler.process(results, start, end)
                            rpc.currentResults = {}
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
        self.logInfo(
            f"Completed: Scanned blocks {start}-{self.endBlock} from {time.asctime(time.localtime(startTime))} to {time.asctime(time.localtime(time.time()))} in {numChunks} chunks"
        )
        self.logInfo(
            f"average {(self.endBlock-start)/(time.time()-startTime)} blocks per second"
        )
        self.teardown()

        

def readConfig(configPath):
    with open(configPath + "config.json") as f:
        cfg = json.load(f)
    return cfg["RPCSETTINGS"], cfg["SCANSETTINGS"], cfg["FILESETTINGS"]


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    scan()

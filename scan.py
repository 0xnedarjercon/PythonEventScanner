from web3 import Web3
import time
import os
from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data
import sys
import json
from web3.providers.rpc import HTTPProvider
from tqdm import tqdm



def main():
    apiURL, maxChunk, startBlock, endBlock, abi, contracts, saveinterval, fileString = readConfig()
    provider = HTTPProvider(apiURL)
    provider.middlewares.clear()
    w3 = Web3(provider)
    eventNames = []
    eventSigs = []
    for event in abi:
        eventNames.append(event['name'])
        inputTypes = [input_abi['type'] for input_abi in event['inputs']]
        eventSigs.append(Web3.sha3(text=f"{eventNames[-1]}({','.join(inputTypes)})").hex())
    if fileString == '':
        fileString = f"Block {startBlock} to {endBlock} events {','.join(eventNames)} contracts {','.join(contracts)}"
    if endBlock == 'latest':
        endBlock = w3.eth.blockNumber
    es = EventScanner(w3, contracts, eventSigs, abi, fileString, saveinterval)
    try:
        es.scan(startBlock, endBlock, maxChunk)
    except KeyboardInterrupt:
        print('keyboard interrupt detected, saving...')
        es.saveState()


class EventScanner:
    def __init__(self, w3, contracts, eventSigs, abis, fileString, saveinterval):
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
            self.topicCounts[eventSigs[x]] = sum(1 for inp in abis[x]['inputs'] if inp['indexed'])+1
        self.fileString = './Output/'+fileString+'.json'
        if os.path.isfile(self.fileString):
            with open(self.fileString, 'r') as f:
                self.log = json.load(f)
                self.latestBlock = self.log['latest']
        else:
            self.log = {'latest' :0}
            

    def scanChunk(self, start, end):
        allEvents = []
        data_filter_set, filter = construct_event_filter_params(
            self.abis[0], self.w3.codec,
            address =self.contracts,
            topics=[self.eventSigs,[]],
            fromBlock=start,
            toBlock=end,
        )
        logs = self.w3.eth.get_logs(filter)

        for log in logs:
            if self.topicCounts[log['topics'][0].hex()]== len(log['topics']):
                evt = get_event_data(self.w3.codec, self.abiLookups[log['topics'][0].hex()], log)
                allEvents.append(evt)
        return allEvents
    

    def scan(self, start, end, maxChunk):
        print(f'starting scan at {time.asctime(time.localtime(time.time()))}')
        if start < self.latestBlock:
            start = self.latestBlock
            print(f'existing file found: {self.fileString}, continuing from {start} to {end}')
        else:
            print(f'no file found at: {self.fileString}, scanning from {start} to {end}')
        startChunk = start  
        endChunk = 0
        startTime = time.time()
        lastSave = startTime
        totalBlocks = end-start
        maxTries = 9
        tries = 0
        successes = 0
        currentChunk = int(maxChunk/2)
        changeAmount = int((maxChunk)/10)
        initialEventCount = len(self.log)
        numChunks = 0
        with tqdm(total=start-end) as progress_bar: 
            
            while endChunk < end and (end-start >0):
                try:
                    if end- startChunk > currentChunk:
                        endChunk = startChunk+currentChunk
                    else:
                        endChunk = end
                    events = self.scanChunk(startChunk, endChunk)
                    if successes>5:
                        currentChunk+= changeAmount
                    if currentChunk>maxChunk:
                        changeAmount = maxChunk
                    tries = 0
                    self.latestBlock = endChunk
                    elapsedTime = time.time()-startTime
                    progress = (startChunk-start)/totalBlocks 
                    if progress >0:             
                        eta = time.asctime(time.localtime((1 - progress) * elapsedTime / progress+startTime))
                    else:
                        eta = 'INF'
                    progress_bar.set_description(
                        f"Current block: {startChunk} , blocks in a scan batch: {currentChunk}, events processed in a batch {len(events)} ETA:{eta}"
                    )
                    progress_bar.update(currentChunk)
                    self.storeLog(events)
                    startChunk = endChunk
                    numChunks+=1
                    if time.time()-lastSave>self.saveInterval:
                        self.saveState()
                        self.backup()
                except ValueError as e:
                    tries+=1
                    successes = 0
                    if tries > maxTries:
                        print('could not get logs, try reducing maxChunk')
                        sys.exit()
                    currentChunk -=  changeAmount
                    print(e, f'\nreducing scan size to {currentChunk} blocks')
        print(f'Scanned total {len(self.log)-initialEventCount} events from {time.asctime(time.localtime(startTime))} to {time.asctime(time.localtime(time.time()))} in {numChunks} chunk')
        self.saveState()

    def storeLog(self, events):
        for param in events:
            blockNumber,txHash, address, index= getParams(param)
            if blockNumber not in self.log:
                self.log[blockNumber] = {}
            if txHash not in self.log[blockNumber]:
                self.log[blockNumber][txHash] = {}
            if address not in self.log[blockNumber][txHash]:
                self.log[blockNumber][txHash][address] = {}
            self.log[blockNumber][txHash][address][index] = {}
            for eventName, eventParam in param["args"].items():
                self.log[blockNumber][txHash][address][index][eventName] = eventParam


    def saveState(self):
        self.log['latest'] = self.latestBlock
        with open(self.fileString, "w") as f:
            f.write(json.dumps(self.log, indent=4))
    def backup(self):   
        with open('./Output/backup.json', "w") as f:
            f.write(json.dumps(self.log, indent=4))

def getParams(param):
    return param['blockNumber'], param["transactionHash"].hex(), param["address"], str(param["event"]) + ' '+str(param["logIndex"])

def readConfig():
    with open("./config.json") as f:
        cfg = json.load(f)
    apiURL = cfg["RPCSETTINGS"]["APIURL"]
    maxChunk = cfg["RPCSETTINGS"]["MAXCHUNKSIZE"]
    startBlock = cfg["SCANSETTINGS"]["STARTBLOCK"]
    endBlock = cfg["SCANSETTINGS"]["ENDBLOCK"]
    abi = cfg["SCANSETTINGS"]["ABI"]
    contracts = cfg["SCANSETTINGS"]["CONTRACTS"]
    fileString = cfg['FILESETTINGS']['FILENAME']
    saveinterval = cfg['FILESETTINGS']["SAVEINTERVAL"]
    return apiURL, maxChunk, startBlock, endBlock, abi, contracts, saveinterval, fileString

if __name__ == "__main__":
    main()

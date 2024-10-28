from web3._utils.events import get_event_data
import sys
from web3 import Web3
import time
import re
from logger import Logger
import math
import asyncio
import traceback
from hardhat import runHardhat
import copy
from multiprocessing.managers import ListProxy, DictProxy
from utils import blocks, toNative
from utils import getW3
from web3 import PersistentConnection
import tracemalloc
tracemalloc.start()
import websockets
# def getW3(cfg):
#     apiURL = cfg["APIURL"]
#     if apiURL[0:3] == "wss":
#         provider = Web3.WebsocketProvider(apiURL)
#         webSocket = True
#     elif apiURL[0:4] == "http":
#         provider = Web3.HTTPProvider(apiURL)
#         provider.middlewares.clear()
#         webSocket = False
#     elif apiURL[0] == "/":
#         provider = Web3.IPCProvider(apiURL)
#         webSocket = False
#     else:
#         print(f"apiUrl must start with wss, http or '/': {apiURL}")
#         sys.exit(1)
#     w3 = Web3(provider)
#     return w3, webSocket




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

    

class RPC(Logger):

    def __init__(self, apiUrl, rpcSettings):
        self.apiUrl = apiUrl
        super().__init__(apiUrl[8:])
        self.apiUrl = apiUrl
        self.maxChunkSize = rpcSettings["MAXCHUNKSIZE"]
        self.currentChunkSize = rpcSettings["STARTCHUNKSIZE"]
        self.eventsTarget = rpcSettings["EVENTSTARGET"]
        self.modes = rpcSettings["MODES"]
        self.lastTime = 0
        self.gasPrice = None
        self.currentJobs = []
        self.running = False
        self.failCount = 0
        self.currentSubScriptions = []

    async def init(self):
        self.w3, self.websocket = await getW3(self.apiUrl)
        return self

    #takes a eth.get_logs job based on its scan parameters and the remaining range to be scanned
    async def doScan(self, remaining, filter, results, jobLock):
        if len(self.currentJobs)==0:
            async with jobLock:
                startBlock = remaining[0]
                endBlock = min(remaining[0] + self.currentChunkSize, remaining[1])
                remaining[0] = endBlock + 1
            filter['fromBlock'] = startBlock
            filter['toBlock'] = endBlock
            self.logInfo(f'took job {blocks(filter)}')
            self.currentJobs.append(filter)
        filter = self.currentJobs.pop(0)
        try:
            result = await self.w3.eth.get_logs(filter)
            if len(result)>0:
                await results.put([filter['fromBlock'] ,result, filter['toBlock']])
                self.logInfo(f'added results {filter}')
            self.logInfo(f'successful job')
            self.failCount = 0
            self.throttle(result, filter['toBlock']-filter['fromBlock'])     
        except Exception as error:
                self.logInfo(f'error with job {error}')
                if self.failCount >20:
                    self.logWarn(f'too many failures, shutting down rpc...')
                    self.currentJobs.append(filter)
                    _min = min(x['fromBlock'] for x in self.currentJobs+[filter])
                    _max = max(x['toBlockBlock'] for x in self.currentJobs+[filter])
                    filter['fromBlock'] = _min
                    filter['toBlock'] = _max

                    await results.put(filter)
                    self.running = False
                    return
                self.handleError([filter, error])


    async def get_logs(self, remaining, filter, results,  jobLock):
        self.running = True
        self.filter = filter = filter.copy()
        async with jobLock:
            self.live = (remaining[1] == 'latest')
        if self.live:
            await self.liveScan(remaining, filter, results,  jobLock)
        else:
            finished = False
            while (self.live or not (finished and len(self.currentJobs) == 0)) and self.running:
                await self.doScan(remaining, filter, results,  jobLock)
                if not self.running:
                    return
                await asyncio.sleep(0) 
                async with jobLock:
                    finished = remaining[0]>= remaining[1]    
        self.running = False
            
    async def liveScan(self, remaining, filter, results, jobLock):
        lastRxBlock =0
        if self.websocket:
            subscriptionId = await self.w3.eth.subscribe("newHeads")
            fails = 0
            while self.live:
                try:
                    self.logInfo(f'checking for new messages, timeout 10')
                    payload = await asyncio.wait_for(self.w3.socket._manager._get_next_message(), 10)
                    self.logInfo(f'message received')
                    result = payload['result']
                    blockNumber = result['number']
                    self.gasPrice = result.baseFeePerGas
                    self.lastTimestamp = result.timestamp
                    async with jobLock:
                        lastBlock = remaining[0]
                    self.logInfo(f'new block {blockNumber}')
                    if lastBlock < blockNumber:
                        self.logInfo(f'processing job: current time{time.time()}, block time: {result.timestamp}, delta: {time.time()-result.timestamp}' )
                        lastRxBlock= await self.processNewEvents(filter, remaining, jobLock, results, lastRxBlock)
                        fails = 0
                    else:
                        self.logInfo(f'not new block, skipping')
                except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError )as e:
                    fails += 0
                    self.logInfo(f'error {e}, restarting w3 {fails}/3')
                    await asyncio.sleep(3)
                    self.w3, self.websocket = await getW3(self.apiUrl)
                    subscriptionId = await self.w3.eth.subscribe("newHeads")
                    if fails >3:
                        self.live = False
                        self.running = False
                        self.logWarn(f'too many errors in rpc, shutting down')
                        return
                except Exception as e:
                    self.logWarn(f'unhandled error in livescan {e}')
            await self.w3.eth.unsubscribe(subscriptionId)          
        else:
            while self.live:
                try:
                    lastRxBlock= await self.processNewEvents(filter, remaining, jobLock, results, lastRxBlock)
                except Exception as e:
                    self.logWarn(f'unhandled error in livescan {e}')
                    
    async def processNewEvents(self, filter, remaining, jobLock, results, lastRxBlock, blockNum = None):
        async with jobLock:
            remaining[0] = max(lastRxBlock-1, remaining[0])
            self.logInfo(f'remaining updated to {remaining}, {lastRxBlock}')
            filter['fromBlock'] = remaining[0]
        result = await self.w3.eth.get_logs(filter)
        if len(result)>0:
            lastRxBlock = result[-1]['blockNumber']
            await results.put([filter['fromBlock'] ,result, lastRxBlock])
            self.logInfo(f'livescan update to {lastRxBlock}')
        # elif blockNum == None:
        #     lastRxBlock = await self.w3.eth.get_block_number()
        #     self.logInfo(f'last getBlockNumber: {lastRxBlock}')
        # else:
        #     lastRxBlock = blockNum
        self.logInfo(f'job success {len(result)} events')

        return lastRxBlock

            
    def throttle(self, events, blockRange):
        if len(events) > 0:
            ratio = self.eventsTarget / (len(events))
            targetBlocks = math.ceil(ratio * blockRange)
            self.currentChunkSize = min(targetBlocks, self.maxChunkSize)
            self.currentChunkSize = max(self.currentChunkSize, 1)
            self.logInfo(
                    f"processed events: {len(events)}, ({blockRange}) blocks, throttled to {self.currentChunkSize}"
                )
    #-----------------------rpc error handling------------------------------------
    
    def handleRangeTooLargeError(self, failingJob):
        try:
            for word in failingJob[-1].args[0]["message"].split(' '):
                word = (word.replace('k', '000'))
                if word[0].isdigit():
                    maxBlock = int(word)
                    if maxBlock > self.currentChunkSize:
                        raise Exception
                    else:
                        filter = failingJob[0]
                        self.maxChunk = maxBlock
                        self.currentChunkSize = maxBlock                                
        except Exception as error:
                    self.maxChunk = int(self.currentChunkSize * 0.95)
                    self.currentChunkSize = min(self.maxChunk, self.currentChunkSize )
        self.splitJob(
                    math.ceil((filter['toBlock']-filter['fromBlock']) / maxBlock), failingJob
                )
        self.logInfo(f"blockrange too wide, reduced max to {self.currentChunkSize}")
    def handleInvalidParamsError(self, failingJob):
        if "Try with this block range" in failingJob[-1].args[0]["message"]:
            match = re.search(
                r"\[0x([0-9a-fA-F]+), 0x([0-9a-fA-F]+)\]", failingJob[-1].args[0]["data"]
            )
            if match:
                start_hex, end_hex = match.groups()
                suggestedLength = int(end_hex, 16) - int(start_hex, 16)
                self.logInfo(
                    f"too many events, suggested range {suggestedLength}"
                )
                self.splitJob(
                    math.ceil(self.currentChunkSize / suggestedLength), failingJob
                )
            else:
                self.logWarn(
                    f"unable to find suggested block range, splitting jobs"
                )
                self.splitJob(2, failingJob)
    def handleResponseSizeExceeded(self, failingJob):
        self.eventsTarget = self.eventsTarget*0.95
        self.splitJob(2, failingJob)
    def handleError(self, failingJob):
            e = failingJob[-1]
            if type(e) == ValueError:
                if e.args[0]["message"] == "block range is too wide" or 'range is too large' in e.args[0]["message"] :
                    self.handleRangeTooLargeError(failingJob)
                elif e.args[0]["message"] == "invalid params" or "response size should not greater than" in e.args[0]["message"]:
                    self.handleInvalidParamsError(failingJob)
                elif "response size exceed" in e.args[0]["message"]:
                    self.handleResponseSizeExceeded(failingJob)
                    
                elif e.args[0]["message"] == "rate limit exceeded":
                    self.logInfo(f"rate limited trying again")     
                else:
                    self.logWarn(
                        f"unhandled error {type(e), e}, {traceback.format_exc()} splitting jobs",
                        True,
                    )
                    self.splitJob(2, failingJob)
                    self.failCount += 1
            elif type(e) == asyncio.exceptions.TimeoutError:
                
                self.logInfo(f"timeout error, splitting jobs")
                self.splitJob(2, failingJob)
                self.failCount += 1
            elif type(e) == KeyboardInterrupt:
                pass
            else:
                self.logWarn(
                    f"unhandled error {type(e), e},{traceback.format_exc()}  splitting jobs",
                    True,
                )
                self.splitJob(2, failingJob)
                self.failCount += 1

    # reduces the scan range by a specified factor, 
    # removes all jobs in the jobmanager, splits them based on the new scan range and adds them back
    def splitJob(self,numJobs, failingJob ,chunkSize =None, reduceChunkSize=True):
        self.logInfo(f'spitting jobs from {blocks(failingJob)}')
        if type(chunkSize) !=(int):
            chunkSize = math.ceil((failingJob[0]['toBlock'] - failingJob[0]['fromBlock']) / numJobs)
        if reduceChunkSize:
            self.currentChunkSize = max(chunkSize, 1)
        filter = failingJob[0]
        currentBlock = filter['fromBlock'] 
        while currentBlock <= failingJob[0]['toBlock']:
            _filter = copy.deepcopy(filter)
            _filter['fromBlock'] = currentBlock
            _filter['toBlock'] = min(currentBlock+chunkSize, filter['toBlock'])
            self.currentJobs.append(_filter)
            currentBlock = _filter['toBlock'] + 1
            self.logInfo(f'added job {blocks(_filter)}')



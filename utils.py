from multiprocessing.managers import ListProxy, DictProxy
from web3._utils.events import get_event_data
from web3 import Web3
import sys

# extracts the blocks from a job
def blocks(filter):
    try:
        if type(filter) == list or type(filter) == ListProxy:
            #get args from job
            filter = filter[1]
        if type(filter) == tuple:
            #get filter from args
            filter = filter[0]
        #return the blocks
        return (filter['fromBlock'], filter['toBlock'], filter['toBlock']-filter['fromBlock'])
    except:
        return 'not get_logs'
    
def getW3(cfg):
    if type(cfg) == dict:
        apiURL = cfg["APIURL"]
    else:
        apiURL = cfg
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
        print(f"apiUrl must start with wss, http or '/': {apiURL}")
        sys.exit(1)
        
    w3 = Web3(provider)
    return w3, webSocket 
# converts multiprocessing types to native types for easier debugging
def toNative(obj):
    # Recursively convert multiprocessing Manager lists and dicts to normal Python types
    if isinstance(obj, ListProxy):
        return [toNative(item) for item in obj]
    elif isinstance(obj, DictProxy):
        return {key: toNative(value) for key, value in obj.items()}
    else:
        return obj
# returns the last block with an event from an eth.get_logs call
def getLastBlock( eventData):
    if len(eventData)>0:
        return eventData[-1]['blockNumber']  
    else:
        return 0  
# formats decoded event data to a nested dictionary of 
# blockNo. txHash, address, index, eventName: eventParams
def getEventData(events):
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
    
# decodes raw event data and returns a nested dictionary of 
# blockNo. txHash, address, index, eventName: eventParams
def decodeEvents(events, scanMode, codec, contracts, abiLookups):
    decodedEvents = []
    if scanMode == "ANYEVENT":
        for event in events:
            evt = get_event_data(
                codec,
                contracts[event["address"]][event["topics"][0].hex()],
                event,
            )
            decodedEvents.append(evt)
    elif scanMode == "ANYCONTRACT":
        for event in events:
            eventLookup = abiLookups[event["topics"][0].hex()]
            numTopics = len(event["topics"])
            if numTopics in eventLookup:
                evt = get_event_data(
                    codec,
                    abiLookups[event["topics"][0].hex()][numTopics],
                    event,
                )
                decodedEvents.append(evt)
    return getEventData(decodedEvents)

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
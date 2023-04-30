from web3 import Web3
from web3.contract import Contract
from web3.datastructures import AttributeDict
from web3.exceptions import BlockNotFound
from eth_abi.codec import ABICodec

from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data
import sys
import json
from web3.providers.rpc import HTTPProvider
from tqdm import tqdm
from web3._utils.abi import (
    exclude_indexed_event_inputs,
    get_abi_input_names,
    get_indexed_event_inputs,
    get_normalized_abi_arg_type,
    map_abi_data,
    normalize_event_input_types,
)


def main():
    with open("./config.json") as f:
        cfg = json.load(f)
    freeapi_url = (
        "https://mainnet.aurora.dev/5STJAuzcDMF4cfMMuupmhDKknK6mj2mxzRUTBqQWjeq"
    )
    apiURL = cfg["APIURL"]
    startBlock = cfg["STARTBLOCK"]
    endBlock = cfg["ENDBLOCK"]
    maxChunk = cfg["MAXCHUNKSIZE"]
    abi = cfg["ABI"]
    contracts = cfg["CONTRACTS"]
    provider = HTTPProvider(apiURL)
    provider.middlewares.clear()
    w3 = Web3(provider)

    PAIR = w3.eth.contract(abi=abi)
    events = []
    for event in abi:
        events.append(PAIR.events[event["name"]])

    es = EventScanner(w3, contracts, events, abi)
    events = es.scan(startBlock, endBlock)


class EventScanner:
    def __init__(self, w3, contracts, events, abi):
        self.w3 = w3
        self.contracts = contracts
        # self.state = state
        self.events = events
        self.abi = abi
        self.log = {}

    def scanChunk(self, start, end, abi, w3):
        allEvents = []
        data_filter_set, filter = construct_event_filter_params(
            abi[0],
            self.w3.codec,
            fromBlock=start,
            toBlock=end,
        )
        logs = w3.eth.get_logs(filter)
        for log in logs:
            evt = get_event_data(self.w3.codec, abi[0], log)
            allEvents.append(evt)
        return allEvents

    def scan(self, start, end):
        events = self.scanChunk(start, end, self.abi, self.w3)

        print(events)
        print("-----------------------------")
        for param in events:
            blockNumber = param["blockNumber"]
            txHash = param["transactionHash"].hex()
            address = param["address"]
            index = str(param["event"]) + str(param["logIndex"])
            if blockNumber not in self.log:
                self.log[blockNumber] = {}
            if txHash not in self.log[blockNumber]:
                self.log[blockNumber][txHash] = {}
            if address not in self.log[blockNumber][txHash]:
                self.log[blockNumber][txHash][address] = {}
            self.log[blockNumber][txHash][address][index] = {}
            for name, eventParam in param["args"].items():
                self.log[blockNumber][txHash][address][index][name] = eventParam

            print(self.log)
            with open("./output.json", "w") as f:
                f.write(json.dumps(self.log, indent=4))


main()

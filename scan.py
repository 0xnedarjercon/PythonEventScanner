from eventScanner import EventScanner, getScanParameters, scan
from dotenv import load_dotenv
import os

load_dotenv()
(
    w3,
    contracts,
    eventSigs,
    abi,
    fileString,
    saveInterval,
    startBlock,
    endBlock,
    maxChunk,
    abiLookups,
    topicCounts,
    websocket,
) = getScanParameters(os.getenv("FOLDER") + "config.json")
scan(
    w3,
    contracts,
    eventSigs,
    abi,
    fileString,
    saveInterval,
    startBlock,
    endBlock,
    maxChunk,
    os.getenv("FOLDER"),
)

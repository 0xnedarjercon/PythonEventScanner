import os
import json
import time
from logger import Logger
import aiofiles



# currently assumes all files stored are sequential
class FileHandler(Logger):
    def __init__(
        self,
        fileSettings,
        configPath,name
    ):
        super().__init__('fh', fileSettings["LOGNAMES"])
        filePath = configPath + "data/"
        os.makedirs(filePath, exist_ok=True)
        self.currentFile = None
        self.currentData = {}
        self.start = 0
        self.filePath = filePath
        self.maxEntries = fileSettings["MAXENTRIES"]
        self.saveInterval = fileSettings["SAVEINTERVAL"]
        self.pending = []
        self.latest = 0
        self.maxBlock = 0
        self.next = None
        self.lastSave = time.time()

    def createNewFile(self, startBlock=None):
        if startBlock is None:
            startBlock = self.latest + 1
        self.start = self.latest = startBlock

        self.currentFile = (self.start, self.latest)
        self.currentData = {}
        self.logInfo(f"new file created starting {self.latest}")

    @property
    def currentFileName(self):
        return f"{self.currentFile[0]}.{self.currentFile[1]}.json"

    def save(self, deleteOld=True, indent=None):
        if self.currentData and self.latest != self.start:
            newName = f"{self.start}.{self.latest}.json"
            with open(self.filePath + newName, "w") as f:
                f.write(json.dumps(self.currentData, indent=indent))
            if deleteOld and newName != self.currentFileName:
                self.logDebug(f"deleting {self.currentFile}")
                try:
                    os.remove(self.filePath + self.currentFileName)
                except FileNotFoundError as e:
                    self.logDebug("filenotfound error deleting {e}")
            self.currentFile = (self.start, self.latest)
            self.lastSave = time.time()
            self.logInfo(f"current data saved to {self.currentFileName}")
        else:
            self.logDebug(f"{self.currentFile} not saved, no changed data")
            
    async def asyncSave(self, deleteOld=True, indent=None):
        if self.currentData and self.latest != self.start:
            newName = f"{self.start}.{self.latest}.json"
            async with aiofiles.open(self.filePath + newName, "w") as f:
                await f.write(json.dumps(self.currentData, indent=indent))
            if deleteOld and newName != self.currentFileName:
                self.logDebug(f"deleting {self.currentFile}")
                try:
                    os.remove(self.filePath + self.currentFileName)
                except FileNotFoundError as e:
                    self.logDebug("file not found error deleting {e}")
            self.currentFile = (self.start, self.latest)
            self.lastSave = time.time()
            self.logInfo(f"current data saved to {self.currentFileName}")
        else:
            self.logDebug(f"{self.currentFile} not saved, no changed data")
    #results array [fromBlock, events, toBlock]
    async def process(self, results, guarunteedContinuous =False):
        if guarunteedContinuous:
            for result in results:
                self.currentData.update(result[1])
                self.latest = max(result[2], self.latest)
            await self.asyncSave()
        else:
            for result in results:
                self.addToPending(result)
            self.mergePending()
        if len(self.currentData) > self.maxEntries:
            await self.asyncSave(indent=4)
            self.createNewFile()
        elif (
            time.time() > self.lastSave + self.saveInterval and self.currentData != None
        ):
            await self.asyncSave()


    def mergePending(self):
        while len(self.pending) > 0 and self.pending[0][0] <= self.latest + 1:
            self.currentData.update(self.pending[0][1])
            self.latest = max(self.pending[0][2], self.latest)
            self.logInfo(
                f"pending merged to current data {self.pending[0][0]} to {self.pending[0][2]}, latest stored: {self.latest}"
            )
            self.pending[0][2] - self.pending[0][0]
            self.pending.pop(0)
        self.logInfo(f"waiting for: {self.latest+1}")


    def addToPending(self, element):
        position = 0
        while position < len(self.pending) and self.pending[position][0] < element[0]:
            position += 1
        self.pending.insert(position, element)
        self.logInfo(f"data added to pending {element[0]} to {element[2]}")

    def getFiles(self):
        all_files = os.listdir(self.filePath)
        json_files = [
            (int(start), int(end))
            for file in all_files
            if file.endswith(".json")
            for start, end in [file.rsplit(".", 2)[:2]]
        ]
        return sorted(json_files, key=lambda x: x[0])

    def getLatestFileFrom(self, startBlock):
        fileTuples = self.getFiles()
        lastFile = None
        for fileTuple in fileTuples:
            if lastFile is None:
                if startBlock <= fileTuple[1]:
                    if startBlock >= fileTuple[0]:
                        lastFile = fileTuple
                    else:
                        break
            else:
                if lastFile[1] + 1 == fileTuple[0]:
                    lastFile = fileTuple
        return lastFile

    def toFileName(self, value):
        return f"{value[0]}.{value[1]}.json"

    def loadFile(self, file):
        with open(f"{self.filePath}{file}") as f:
            return json.load(f)

    def setup(self, startBlock):
        self.logInfo(f"setting up new scan")
        if self.currentFile != None:
            self.save(indent=4)
        latestFileTuple = self.getLatestFileFrom(startBlock)
        if latestFileTuple is None:
            self.createNewFile(startBlock)
            self.latest = startBlock
            self.start = startBlock
            self.currentFile = (self.start, self.latest)
            self.currentData = {}
        else:
            self.start = latestFileTuple[0]
            self.latest = latestFileTuple[1]
            self.currentFile = (self.start, self.latest)
            self.currentData = self.loadFile(self.currentFileName)

        self.logDebug(f"setup complete, {self.currentFile} waiting for {self.latest}")
        return self.latest

    def checkMissing(self, start, end):
        files = self.getFiles()
        missing = []
        i = 0
        started = False
        while start < end and i < len(files):
            if not started:
                if files[i][0] > start:
                    missing.append((start, files[i][0] - 1))
                    started = True
                elif files[i][1] >= start:
                    started = True
            else:
                if files[i][0] > start:
                    missing.append((start, files[i][0] - 1))
            start = max(files[i][1] + 1, start)
            i += 1
        if start < end:
            missing.append((start, end))
        self.logDebug(f"missing files: {missing}")
        return missing

    def getEvents(self, start, end, results = {}):
        files = self.getFiles()
        for file in files:
            if file[1] >= start:
                data = self.loadFile(self.toFileName(file))
                if file[0]< start:
                    data = {key:value for key, value in data.items() if int(key) >= start}
                if file[1]> end:
                    data = {key:value for key, value in data.items() if int(key) <= end}
                    results.append(data)
                    return results
                results.update(data)
        return results

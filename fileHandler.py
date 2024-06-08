import os
import json
import time
from logger import Logger
from configLoader import fileSettings, configPath


# currently assumes all files stored are sequential
class FileHandler(Logger):
    def __init__(self):
        super().__init__("fileHandler", fileSettings["DEBUGLEVEL"])
        filePath = configPath + fileSettings["FILENAME"]+'/'
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

    def createNewFile(self, startBlock = None):
        if startBlock is None:
            startBlock=self.latest
        self.start = self.latest =startBlock 

        self.currentFile = (self.start,self.latest)
        self.currentData = {}
        self.logInfo(f"new file created starting {self.latest}")
    
    @property
    def currentFileName(self):
        return f'{self.currentFile[0]}.{self.currentFile[1]}.json'
    
    def save(self, deleteOld=True, indent = None):
        if self.currentData and self.latest != self.start:
            newName = f'{self.start}.{self.latest}.json'
            with open(self.filePath+newName, "w") as f:
                f.write(json.dumps(self.currentData, indent=indent))
            if deleteOld and newName != self.currentFileName:
                self.logDebug(f'deleting {self.currentFile}')
                try:
                    os.remove(self.filePath+self.currentFileName)
                except FileNotFoundError as e:
                    self.logDebug('filenotfound error deleting {e}')
            self.currentFile = (self.start, self.latest)
            self.lastSave = time.time()
            self.logInfo(f"current data saved to {self.currentFileName}")
        else:
            self.logDebug(f'{self.currentFile} not saved, no changed data')
        
    def process(self, data):
        self.addToPending(data)
        numBlocks = self.mergePending()
        if len(self.currentData) > self.maxEntries:
            self.save(indent = 4)
            self.createNewFile()
        elif (
            time.time() > self.lastSave + self.saveInterval and self.currentData != None
        ):
            self.save()
        return numBlocks

    def mergePending(self):
        numBlocks = 0
        while len(self.pending) > 0 and self.pending[0][0] <= self.latest:
            self.currentData.update(self.pending[0][1])
            self.latest = self.pending[0][2]
            self.logInfo(
                f"pending merged to current data {self.pending[0][0]} to {self.pending[0][2]}, latest stored: {self.latest}"
            )
            numBlocks += self.pending[0][2] - self.pending[0][0]
            self.pending.pop(0)
        self.logInfo(
                f"waiting for: {self.latest}"
            )
        return numBlocks

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
            for start, end in [file.rsplit('.', 2)[:2]]
        ]
        return sorted(json_files, key=lambda x: x[0])
                
    def getLatestFileFrom(self, startBlock):
        fileTuples = self.getFiles()
        fileEnd = startBlock
        for i, fileTuple in enumerate(fileTuples):
            if fileEnd == startBlock  and startBlock >= fileTuple[0]:
                fileEnd = fileTuple[1]
            if fileEnd != startBlock:
                if i + 1 == len(fileTuples):
                    if startBlock> fileTuple[1]:
                        return None
                    return fileTuple
                if fileEnd != fileTuples[i + 1][0]:
                    return fileTuples[i + 1]
        return None
    
    def toFileName(self, value):
        return f'{value[0]}.{value[1]}.json'
    
    def loadFile(self, file):
        with open(f'{self.filePath}{file}') as f:
            return json.load(f)
        
    def setup(self, startBlock):
        self.logInfo(f'setting up new scan')
        if self.currentFile != None:
            self.save(indent = 4)
        self.createNewFile(startBlock)
        self.logDebug(f'setup complete, {self.currentFile} waiting for {self.latest}')
        return self.latest

        
    def checkMissing(self, start, end):
        files = self.getFiles()
        missing = []
        i=0
        while start < end and i < len(files):
            if files[i][0]>start:
                missing.append((start, files[i][0]))
            start = files[i][1]
            i+=1
        if start< end:
            missing.append((start, end))
        self.logDebug(f'missing files: {missing}')
        return missing

    def getEvents(self, start, end, results):
        files = self.getFiles()
        foundStart = False
        for file in files:
            if file[1] < start:
                continue
            data = self.loadFile(self.toFileName(file))
            # Filter the first matching file
            if not foundStart:
                data = {k: v for k, v in data.items() if int(k) >= start}
                foundStart = True
            # Filter the last matching file
            if file[1] > end:
                data = {k: v for k, v in data.items() if int(k) <= end}
                results.append(data)
                break
            results.append(data)
        return results
                



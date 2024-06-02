import os
import json
import time
from logger import Logger
from configLoader import fileSettings, configPath


class FileHandler(Logger):
    def __init__(self, startBlock):
        super().__init__("fileHandler", fileSettings["DEBUGLEVEL"])
        filePath = configPath + fileSettings["FILENAME"]
        os.makedirs(filePath, exist_ok=True)
        self.currentFile = None
        self.latest = startBlock
        self.filePath = filePath
        self.maxEntries = fileSettings["MAXENTRIES"]
        self.saveInterval = fileSettings["SAVEINTERVAL"]
        self.pending = []
        self.openLatest()
        self.lastSave = time.time()

    def createNewFile(self):
        self.currentFile = os.path.join(self.filePath, str(self.latest) + ".json")
        self.currentData = {"latest": self.latest}
        self.save()
        self.logInfo(f"new file created starting {self.latest}")

    def process(self, data):
        self.addToPending(data)
        numBlocks = self.mergePending()
        if len(self.currentData) > self.maxEntries:
            self.save()
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
            self.currentData["latest"] = self.latest = self.pending[0][2]
            self.logInfo(
                f"pending merged to current data {self.pending[0][0]} to {self.pending[0][2]}, latest stored: {self.latest}"
            )
            numBlocks += self.pending[0][2] - self.pending[0][0]
            self.pending.pop(0)
        return numBlocks

    def save(self):
        with open(self.currentFile, "w") as f:
            f.write(json.dumps(self.currentData, indent=4))
        self.lastSave = time.time()
        self.logInfo("current data saved")

    def addToPending(self, element):
        position = 0
        while position < len(self.pending) and self.pending[position][0] < element[0]:
            position += 1
        self.pending.insert(position, element)
        self.logInfo(f"data added to pending {element[0]} to {element[2]}")

    def openLatest(self):
        try:
            latestFile = self.getFiles()[-1]
            self.currentFile = os.path.join(self.filePath, latestFile)
            with open(self.currentFile) as f:
                self.currentData = json.load(f)
            if self.currentData["latest"] > self.latest:
                self.latest = self.currentData["latest"]
                self.logInfo(f"opened file {latestFile}")
            else:
                latestFile = self.createNewFile()
        except:
            latestFile = self.createNewFile()

    def getFiles(self):
        all_files = os.listdir(self.filePath)
        return sorted(
            (
                file
                for file in all_files
                if file.endswith(".json") and file[:-5].isdigit()
            ),
            key=lambda x: int(x[:-5]),
        )

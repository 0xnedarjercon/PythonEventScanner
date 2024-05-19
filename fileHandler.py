import os
import json
import time


class FileHandler:
    def __init__(self, filePath, eventScanner, maxEntries=100000, saveInterval=6000):

        self.es = eventScanner
        self.currentFile = None
        self.latest = 0
        self.filePath = filePath
        self.maxEntries = maxEntries
        self.saveInterval = saveInterval
        self.pending = []
        self.openLatest()

        self.lastSave = time.time()

    def createNewFile(self):
        self.currentFile = os.path.join(self.filePath, str(self.latest) + ".json")
        self.currentData = {"latest": self.latest}
        self.save()
        self.es.log.info(f"new file created starting {self.latest}")

    def log(self, data, startBlock, lastBlock):
        self.addToPending([startBlock, data, lastBlock])
        self.mergePending()
        if len(self.currentData) > self.maxEntries:
            self.save()
            self.createNewFile()
        elif (
            time.time() > self.lastSave + self.saveInterval and self.currentData != None
        ):
            self.save()

    def mergePending(self):
        while len(self.pending) > 0 and self.pending[0][0] <= self.latest:
            self.currentData.update(self.pending[0][1])
            self.currentData["latest"] = self.latest = self.pending[0][2]
            self.es.log.info(
                f"pending merged to current data {self.pending[0][0]} to {self.pending[0][2]}"
            )
            self.pending.pop(0)

    def save(self):
        with open(self.currentFile, "w") as f:
            f.write(json.dumps(self.currentData, indent=4))
        self.lastSave = time.time()
        self.es.log.info("current data saved")

    def addToPending(self, element):
        position = 0
        while position < len(self.pending) and self.pending[position][0] < element[0]:
            position += 1
        self.pending.insert(position, element)
        self.es.log.info(f"data added to pending {element[0]} to {element[2]}")

    def openLatest(self):
        try:
            latestFile = self.getFiles()[-1]
            self.currentFile = os.path.join(self.filePath, latestFile)
            with open(self.currentFile) as f:
                self.currentData = json.load(f)
            if self.currentData["latest"] > self.es.startBlock:
                self.latest = self.currentData["latest"]
                self.es.log.info(f"opened file {latestFile}")
            else:
                self.latest = self.es.startBlock
                latestFile = self.createNewFile()
        except:
            self.latest = self.es.startBlock
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

    def getEvent(self, fromBlock, toBlock, event):
        files = self.getFiles()
        for i in range(len(files)):
            pass


if __name__ == "__main__":
    fh = FileHandler("./PythonEventScanner/settings/base/basescan2", 10000, 6000)

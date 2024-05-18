import os
import json
import time

class FileHandler():
    def __init__(self, filePath, maxEntries = 100000, saveInterval=6000):
        try:
            os.mkdir(filePath)
        except:
            pass
        self.currentFile = None
        self.latest = 0
        self.filePath = filePath
        self.maxEntries = maxEntries
        self.saveInterval = saveInterval
        self.openLatest()
        self.lastSave = time.time()
    
    def createNewFile(self):
        self.currentFile = os.path.join(self.filePath, str(self.latest)+'.json')
        self.currentData = {"latest": self.latest}
        self.save()

    def log(self, data, lastBlock):
        self.currentData.update(data) 
        self.currentData["latest"] = self.latest = lastBlock
        if len(self.currentData)> self.maxEntries:
            self.save()
            self.createNewFile()
        elif time.time() >self.lastSave+self.saveInterval and self.currentData != None:
            self.save()
            

    def save(self):
        with open(self.currentFile, 'w' ) as f:
            f.write(json.dumps(self.currentData, indent= 4))
        self.lastSave = time.time()
            
    def openLatest(self):
        try:
            latestFile = self.getFiles()[-1]
            self.currentFile = os.path.join(self.filePath, latestFile)
            with open(self.currentFile) as f:
                self.currentData = json.load(f)
            self.latest = self.currentData["latest"]
        except:
            latestFile = self.createNewFile()
    
    def getFiles(self):
        all_files = os.listdir(self.filePath)
        return sorted(
            (file for file in all_files if file.endswith('.json') and file[:-5].isdigit()),
            key=lambda x: int(x[:-5])
    )
    def getEvent(self, fromBlock, toBlock, event):
        files = self.getFiles()
        for i in range(len(files)):
            pass
            
if __name__ == "__main__":
    fh = FileHandler('./PythonEventScanner/base/output/basescan', 10000, 6000)

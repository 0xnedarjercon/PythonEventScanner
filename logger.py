import logging


import os

log_levels = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class Logger:

    def __init__(self, name, settings, configPath):
        debugLevel = settings["DEBUGLEVEL"]
        name = name.split("/")[-1]
        if debugLevel == None:
            self.enabled = False
        else:
            self.enabled = True
            self.log = logging.getLogger(name)
            self.log.setLevel(debugLevel)
            os.makedirs(configPath + "logs/", exist_ok=True)
            # Create a file handler specific to this logger
            fh = logging.FileHandler(configPath + f"logs/{name}.log")
            fh.setLevel(debugLevel)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            fh.setFormatter(formatter)
            self.log.addHandler(fh)

    def logDebug(self, data, display=False):
        if self.enabled:
            self.log.debug(data)
        if display:
            print(data)

    def logInfo(self, data, display=False):
        if self.enabled:
            self.log.info(data)
        if display:
            print(data)

    def logWarn(self, data, display=False):
        if self.enabled:
            self.log.warning(data)
        if display:
            print(data)

    def logCritical(self, data, display=False):
        if self.enabled:
            self.log.critical(data)
        if display:
            print(data)

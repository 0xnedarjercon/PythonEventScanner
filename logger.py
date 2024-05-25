import logging

log_levels = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
import os


class Logger:
    def __init__(self, name, enabled):
        self.log = logging.getLogger(name)
        self.name = name
        self.enabled = enabled

    @staticmethod
    def initLog(filePath, level):
        try:
            os.mkdir(filePath)
        except:
            pass
        # Ensure basicConfig is called only once to set up the logging configuration
        logging.basicConfig(
            filename=filePath + "/debug.log",  # Logfile name
            filemode="a",  # Append mode
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Log format
            level=log_levels.get(level.upper(), logging.INFO),  # Log level
        )
        print(f"logging to: {filePath}")
        logging.info("Logging initialized")

    def logDebug(self, data):
        if self.enabled:
            self.log.debug(data)

    def logInfo(self, data):
        if self.enabled:
            self.log.info(data)

    def logWarn(self, data):
        if self.enabled:
            self.log.warning(data)

    def logCritical(self, data):
        if self.enabled:
            self.log.critical(data)

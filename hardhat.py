from web3 import Web3
import subprocess
import atexit
import time
from web3 import Web3
from logger import Logger


def runHardhat(configPath, cfg):
    logName = configPath + "logs/" + cfg["LOGNAME"] + ".log"
    command = ["npx", "hardhat", "node"]
    for arg, value in cfg["ARGS"].items():
        command.append("--" + arg)
        command.append(str(value))
    std = subprocess.DEVNULL
    file = None
    if cfg["DEBUGLEVEL"] == "HIGH":
        file = open(logName, cfg["LOGMODE"])
        std = file
    process = subprocess.Popen(command, stdout=std, stderr=subprocess.DEVNULL)
    atexit.register(terminate_process, process, file)
    time.sleep(5)
    return process


def terminate_process(process, file):
    if file is not None:
        file.close()
    if process.poll() is None:  # Check if the process is still running
        process.terminate()
        try:
            process.wait(timeout=5)  # Wait for the process to terminate gracefully
        except subprocess.TimeoutExpired:
            process.kill()  # Force kill if it does not terminate in time

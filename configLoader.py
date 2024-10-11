import json
from dotenv import load_dotenv
import os


load_dotenv()
folderPath = os.getenv("FOLDER_PATH")
configPath = f"{os.path.dirname(os.path.abspath(__file__))}/settings/{folderPath}/"

def overrideSettings(cfg, rpcSetting):
    for key, value in cfg["RPCOVERRIDE"].items():
        if value != "":
            rpcSetting[key] = value



def loadConfig(config):
    if isinstance(config, dict):
        cfg = config

    else:
        with open(config) as f:
            cfg = json.load(f)
    fileSettings = cfg["FILESETTINGS"]
    scanSettings = cfg["SCANSETTINGS"]
    rpcSettings = cfg["RPCSETTINGS"]
    web3Settings = cfg['W3SETTINGS']

    for rpcSetting in rpcSettings:
        overrideSettings(cfg, rpcSetting)
    return fileSettings, scanSettings, rpcSettings,web3Settings


cfg = loadConfig(configPath+'config.json')
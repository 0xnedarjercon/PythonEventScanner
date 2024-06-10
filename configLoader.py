import json
from dotenv import load_dotenv
import os


load_dotenv()
folderPath = os.getenv("FOLDER_PATH")
configPath = f"{os.path.dirname(os.path.abspath(__file__))}/settings/{folderPath}/"
with open(configPath + "config.json") as f:
    cfg = json.load(f)


fileSettings = cfg["FILESETTINGS"]
scanSettings = cfg["SCANSETTINGS"]
rpcSettings = cfg["RPCSETTINGS"]
rpcInterfaceSettings = cfg["RPCINTERFACE"]
hreSettings = cfg["HRE"]


def overrideSettings(rpcSetting):
    for key, value in cfg["RPCOVERRIDE"].items():
        if value != "":
            rpcSetting[key] = value
    if rpcSetting["APIURL"] in hreSettings.keys():
        rpcSetting["APIURL"] = hreSettings[rpcSetting["APIURL"]]


for rpcSetting in rpcSettings:
    overrideSettings(rpcSetting)
overrideSettings(scanSettings["RPC"])

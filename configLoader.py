import json
from dotenv import load_dotenv
import os


def overrideSettings(cfg, rpcSetting, hreSettings):
    for key, value in cfg["RPCOVERRIDE"].items():
        if value != "":
            rpcSetting[key] = value
    # if rpcSetting["APIURL"] in hreSettings.keys():
    #     rpcSetting["APIURL"] = hreSettings[rpcSetting["APIURL"]]


def loadConfig(config):
    if isinstance(config, dict):
        cfg = config

    else:
        with open(config) as f:
            cfg = json.load(f)
    fileSettings = cfg["FILESETTINGS"]
    scanSettings = cfg["SCANSETTINGS"]
    rpcSettings = cfg["RPCSETTINGS"]
    rpcInterfaceSettings = cfg["RPCINTERFACE"]
    hreSettings = cfg["HRE"]
    web3Settings = cfg['W3SETTINGS']

    for rpcSetting in rpcSettings:
        overrideSettings(cfg, rpcSetting, hreSettings)
    return fileSettings, scanSettings, rpcSettings, rpcInterfaceSettings, hreSettings,web3Settings

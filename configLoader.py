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

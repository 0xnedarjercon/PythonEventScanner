from web3 import Web3
import subprocess
import atexit
import time
from web3 import Web3
from logger import Logger


def runHardhat(cfg):
    command = ["npx", "hardhat", "node"]
    for arg, value in cfg["ARGS"].items():
        command.append("--" + arg)
        command.append(str(value))
    output_file = open('hardhat_output.log', 'a')
    command = ['npx', 'hardhat', 'node', '--fork', 'https://rpc2.fantom.network']
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
    # Capture the output (optional)
    stdout, stderr = process.communicate()
    
    if stdout:
        print(f"Output: {stdout}")
    if stderr:
        print(f"Errors: {stderr}")
        
    stdout, stderr = process.communicate()
    
    if stdout:
        print(f"Output: {stdout}")
    if stderr:
        print(f"Errors: {stderr}")
    from web3 import Web3
    from PythonEventScanner.hardhat import runHardhat
    # Connect to the local Hardhat node (default RPC URL)
    cfg = {
        "ARGS": {
            "port": 8545,
            "fork": "https://base.llamarpc.com"
        },
        "LOG": True,
        "LOGNAMES": "hh_llama",
        "LOGMODE": "a"
        }
    # p = runHardhat(cfg)
    hardhat_url = "http://127.0.0.1:8545"
    web3 = Web3(Web3.HTTPProvider(hardhat_url))

    # Check if the connection is successful
    if web3.provider.is_connected():
        print("Successfully connected to the Hardhat node!")
    

        
        # Optional: Get the latest block number
        latest_block = web3.eth.block_number
        print(f"Latest block number: {latest_block}")
    else:
        print("Failed to connect to the Hardhat node.")
        atexit.register(terminate_process, process)
        return process


def terminate_process(process):
    if process.poll() is None:  # Check if the process is still running
        process.terminate()
        try:
            process.wait(timeout=5)  # Wait for the process to terminate gracefully
        except subprocess.TimeoutExpired:
            process.kill()  # Force kill if it does not terminate in time

def getAccounts():
    from eth_account import Account
    accounts = []
    pks = ['0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80', '0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a','0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6']
    for pk in pks:
        accounts.append(Account.from_key(pk))
    return accounts
        
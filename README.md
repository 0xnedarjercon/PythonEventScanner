# python-EventScanner

Inspired from https://web3py.readthedocs.io/en/stable/examples.html

Simplified the code  
Automatically saves as configured filename or uniquley for given input parameters so multiple scans can be performed
Runs out of folder location configured by .env  
Added config json to easily configure events and contracts to filter for  
Improved (I think) the throttling mechanism and made it easily configurable via config json
Autosaves and restores a backup file if needed  
Save file on keyboard interrupt

pip install -r requirements.txt
copy template folder and rename, update config.json file to your needs explanations are in configInstructions
update .env and setup with new folder path
run the following command or directly from IDE:
python ./eventScanner.py


Multi web3 class will spin up processes for each rpc provided and one for itself if configured

Scanner will create its own Multi web3 instance on creation if one is not passed to be run on the same thread/process

To use a hardhat instance first spin up the node with npx hardhat node + any params e.g --fork and  configure the rpc apiUrl to the server address e.g 'http://127.0.0.1:8545'.
To send specific commands to the hardhat instances use target = multiWeb3.getHREMask()

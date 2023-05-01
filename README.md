# python-EventScanner

Inspired from https://web3py.readthedocs.io/en/stable/examples.html

Simplified the code
Automatically saves as configured filename or uniquley for given input parameters so multiple scans can be performed
Added config json to easily configure events and contracts to filter for
Improved (I think) the throttling mechanism and made it easily configurable via config json
Save file on keyboard interrupt

pip install web3
Setup config.json file
python ./scan.py

"RPCSETTINGS": {
"APIURL": provider URL e.g "https://mainnet.aurora.dev",
"MAXCHUNKSIZE": maximum number of blocks to request at once
"SUCCESSCOUNT": number of consecutive successful requests before increasing number of blocks per request
"CHANGEFACTOR": the % to adjust the number of blocks per request
"MAXTRIES": maximum number of retries before aborting
},
"FILESETTINGS": {
"SAVEINTERVAL": how often to save in seconds
"FILENAME": save as a specific file name, auto generated from input params if ""
},
"SCANSETTINGS": {
"STARTBLOCK": starting block to begin scan
"ENDBLOCK": last block to scan, 'latest' for the current block
"CONTRACTS": array of contract addresses to filter for, leave as [] for all contracts.
"ABI": ABI of the events to filter for, only include events that you want.
}

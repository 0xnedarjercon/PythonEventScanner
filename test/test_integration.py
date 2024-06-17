import sys
import os

current_file_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_file_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
from hypothesis import given, settings, Phase
from stubs.w3Stubs import StubbedEventScanner
from strategies import randomConfig, existingData, randomBlock
from configLoader import loadConfig
import traceback
import json
import shutil
import pickle

settings.register_profile("ci", max_examples=30)
settings.load_profile("ci")


def loadData(path):
    data = {}
    with open(f"test/comparisonData/testData.pkl", "rb") as f:
        logs = pickle.load(f)
    for log in logs:
        if log.blockNumber not in data:
            data[log.blockNumber] = []
        data[log.blockNumber].append(log)
    return data


def loadComparisonData():
    with open(f"test/tmp/comparisonData/15612380.15617777.json") as f:
        data = json.load(f)
    return data


def initDataState(path, blocks):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        print(path)
    os.makedirs(path, exist_ok=False)
    data = loadComparisonData()
    i = 0
    blocks.sort()
    while i < len(blocks) - 1:
        print(f"/{blocks[i]}.{blocks[i+1]-1}.json")
        with open(path + f"/{blocks[i]}.{blocks[i+1]-1}.json", "w") as f:
            f.write(
                json.dumps(
                    {
                        key: value
                        for key, value in data.items()
                        if int(key) >= blocks[i] and int(key) < blocks[i + 1]
                    }
                )
            )
        i += 2


def saveTestData(data, errors):
    data["FAILURES"] = f"{errors}"
    try:
        with open("./test/tmp/failedTests.json", "r") as f:
            existingData = json.load(f)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        existingData = []
    existingData.append(data)
    with open("./test/tmp/failedTests.json", "w") as f:
        f.write(json.dumps(existingData))


@settings(phases=[Phase.generate, Phase.target])
@given(data=randomConfig)
def test_dataAlwaysComplete(data):
    runTest(data)


def runTest(data):
    configPath = "./test/tmp/"
    errors = []
    initDataState(configPath + "data", data["EXISTINGBLOCKS"])
    fileSettings, scanSettings, rpcSettings, rpcInterfaceSettings, hreSettings = (
        loadConfig(data)
    )
    try:
        stubbedEventScanner = StubbedEventScanner(
            fileSettings,
            scanSettings,
            rpcSettings,
            rpcInterfaceSettings,
            configPath,
            showprogress=False,
        )
        stubbedEventScanner.scanBlocks()
    except KeyboardInterrupt:
        # this exception happens when w3.eth runs out of data and is used to stop the scan
        validate(stubbedEventScanner, scanSettings, errors, configPath)
    except Exception as e:
        errors.append((e, traceback.format_exc()))
    if len(errors) > 0:
        saveTestData(data, errors)
        assert len(errors) == 0, f"{errors}"
    else:
        print("all tests passed")


def validate(stubbedEventScanner, scanSettings, errors, configPath):

    fileTuples = stubbedEventScanner.fileHandler.getFiles()
    processedBlocks = {}
    comparisonData = loadComparisonData()
    prevFileEnd = scanSettings["STARTBLOCK"]
    started = False
    for fileTuple in fileTuples:
        if fileTuple[1] >= prevFileEnd:
            # check data continuity
            if not started:
                started = True
                prevFileEnd = fileTuple[0] - 1

            fileStart = fileTuple[0]
            if fileStart - prevFileEnd != 1:
                errors.append(f"gap in files: {fileStart} - {prevFileEnd}")
            fileEnd = fileTuple[1]
            # check data matches expected
            with open(configPath + f"./data/{fileTuple[0]}.{fileTuple[1]}.json") as f:
                data = json.load(f)
            data = {
                key: value
                for key, value in data.items()
                if int(key) >= scanSettings["STARTBLOCK"]
            }
            refData = {
                key: value
                for key, value in comparisonData.items()
                if int(key) >= fileStart
                and int(key) <= fileEnd
                and int(key) >= scanSettings["STARTBLOCK"]
            }
            diffs = compare_dicts(data, refData)
            if len(diffs) > 0:
                errors.append(f"data in {fileTuple} mismatch with reference, {diffs} ")
            # check duplicated data
            duplicates = {
                key: value for key, value in data.items() if key in processedBlocks
            }

            if len(duplicates) > 0:
                errors.append(f"duplicate data found: {duplicates.keys()}")
            processedBlocks.update(data)
            prevFileEnd = fileEnd


# (15616818, 15617754)
#  15616745


def compare_dicts(dict1, dict2, path=""):
    differences = []

    # Get all keys from both dictionaries
    all_keys = set(dict1.keys()).union(set(dict2.keys()))

    for key in all_keys:
        # Build the current path
        current_path = f"{path}.{key}" if path else key

        if key not in dict1:
            differences.append(
                f"Key '{current_path}' is missing in the first dictionary"
            )
        elif key not in dict2:
            differences.append(
                f"Key '{current_path}' is missing in the second dictionary"
            )
        else:
            # Compare values or recurse into nested dictionaries
            if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                # Recursive call for nested dictionaries
                sub_differences = compare_dicts(dict1[key], dict2[key], current_path)
                differences.extend(sub_differences)
            elif dict1[key] != dict2[key]:
                differences.append(
                    f"Value for key '{current_path}' differs: {dict1[key]} != {dict2[key]}"
                )

    return differences


if __name__ == "__main__":
    import json
    import pickle

    testIndex = 0
    with open("./test/tmp/failedTests.json", "r") as f:
        testData = json.load(f)
    if testIndex < 0:
        cfg = randomConfig.example()
    else:
        cfg = testData[testIndex]

    runTest(cfg)

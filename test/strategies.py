from hypothesis.strategies import (
    text,
    integers,
    booleans,
    composite,
    lists,
    fixed_dictionaries,
    floats,
    sampled_from,
    just,
)

MINBLOCK = 15612380
MAXBLOCK = 15617777
randomBlock = integers(max_value=MAXBLOCK, min_value=MINBLOCK)
debugLevels = ["NONE", "EXTREME"]
existingData = lists(
    integers(min_value=MINBLOCK, max_value=MAXBLOCK), min_size=0, max_size=20
).map(lambda lst: list(set(lst)))
maxBlocks = integers(min_value=1000, max_value=10000)
maxEvents = integers(min_value=3000, max_value=30000)

rpcConfig = fixed_dictionaries(
    {
        "NAME": text(min_size=1, max_size=20),
        "APIURL": text(min_size=1, max_size=20),
        "MAXCHUNKSIZE": integers(min_value=3000, max_value=30000),
        "STARTCHUNKSIZE": integers(min_value=1, max_value=120),
        "EVENTSTARGET": integers(min_value=1000, max_value=3000),
        "POLLINTERVAL": just(0),
        "DEBUGLEVEL": sampled_from(debugLevels),
        "LOGNAMES": just(["test"]),
        "ACTIVESTATES": sampled_from([[1], [2], [1, 2]]),
        # fields only used for test stubbed w3.eth
        "STUBSTARTBLOCK": integers(min_value=MAXBLOCK - 200, max_value=MAXBLOCK),
        "STUBMAXBLOCKS": maxBlocks,
        "STUBMAXEVENTS": maxEvents,
    }
)

rpcOverride = {
    "APIURL": "",
    "MAXCHUNKSIZE": "",
    "STARTCHUNKSIZE": "",
    "EVENTSTARGET": "",
    "POLLINTERVAL": "",
    "DEBUGLEVEL": "",
    "ACTIVESTATES": "",
}

fileSettings = fixed_dictionaries(
    {
        "SAVEINTERVAL": integers(min_value=2, max_value=10),
        "FILENAME": just("basescan"),
        "MAXENTRIES": integers(
            min_value=int((MAXBLOCK - MINBLOCK) / 10),
            max_value=MAXBLOCK - MINBLOCK + 100,
        ),
        "DEBUGLEVEL": sampled_from(debugLevels),
        "LOGNAMES": just(["test"]),
    }
)

scanSettings = fixed_dictionaries(
    {
        "NAME": just("scanner"),
        "RPC": just(True),
        "STARTBLOCK": integers(min_value=MINBLOCK, max_value=MAXBLOCK),
        "ENDBLOCK": just("latest"),
        "LIVETHRESHOLD": integers(min_value=0, max_value=200),
        "DEBUGLEVEL": sampled_from(debugLevels),
        "LOGNAMES": just(["test"]),
        "CONTRACTS": just(
            {
                "0x78b3C724A2F663D11373C4a1978689271895256f": "ERC20",
                "0x1a9f461a371559f82976fa18c46a6a0d29f131d0": "AEROPAIR",
            }
        ),
        "MODE": just("ANYCONTRACT"),
        "EVENTS": just(["Sync"]),
    }
)

rpcInterface = fixed_dictionaries(
    {
        "DEBUGLEVEL": sampled_from(debugLevels),
        "LOGNAMES": just(["test"]),
    }
)

hre = fixed_dictionaries(
    {
        "hre_llama": fixed_dictionaries(
            {
                "ARGS": fixed_dictionaries(
                    {"port": just(8454), "fork": just("https://base.llamarpc.com")}
                ),
                "DEBUGLEVEL": sampled_from(debugLevels),
                "LOGNAME": just("hh_llama"),
                "LOGMODE": just("w"),
            }
        )
    }
)

# Define the randomConfig strategy
randomConfig = fixed_dictionaries(
    {
        "RPCSETTINGS": lists(rpcConfig, min_size=1, max_size=6),
        "RPCOVERRIDE": just(rpcOverride),
        "SCANSETTINGS": scanSettings,
        "RPCINTERFACE": rpcInterface,
        "FILESETTINGS": fileSettings,
        "HRE": hre,
        "EXISTINGBLOCKS": existingData,
    }
)

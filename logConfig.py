import logging

logConfig = {
    "NORMAL": {
        "DEBUGLEVEL": logging.WARNING,
        "FORMAT": "%(asctime)s , %(levelname)s , %(message)s",
    },
    "HIGH": {
        "DEBUGLEVEL": logging.INFO,
        "FORMAT": "%(asctime)s, %(name)s, %(levelname)s, %(message)s, %(exc_info)s",
    },
    "EXTREME": {
        "DEBUGLEVEL": logging.DEBUG,
        "FORMAT": "%(asctime)s, %(name)s, %(levelname)s, %(message)s, %(exc_info)s",
    },
}



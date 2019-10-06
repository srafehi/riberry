import logging

# noinspection PyTypeChecker
root_name: str = None

# noinspection PyTypeChecker
logger: logging.Logger = None


def make(name: str) -> logging.Logger:
    return logging.getLogger(name=name)


def init():
    logger.addHandler(logging.NullHandler())

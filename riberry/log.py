import logging

# noinspection PyTypeChecker
root_name: str = None

# noinspection PyTypeChecker
root: logging.Logger = None


def make(name: str) -> logging.Logger:
    return logging.getLogger(name=name)

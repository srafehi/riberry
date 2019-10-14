import logging
import os
import sys

# noinspection PyTypeChecker
root_name: str = None

# noinspection PyTypeChecker
logger: logging.Logger = None
stdout_handler = logging.StreamHandler(stream=sys.stdout)


def make(name: str) -> logging.Logger:
    return logging.getLogger(name=name)


def init():
    logger.addHandler(logging.NullHandler())
    stdout_handler.formatter = logging.Formatter(
        os.environ.get('RIBERRY_LOGFORMAT', '%(levelname)-8s %(asctime)-15s %(name)s - %(message)s')
    )
    if os.environ.get('RIBERRY_LOGLEVEL'):
        logger.setLevel(level=os.environ['RIBERRY_LOGLEVEL'].upper())
        logger.addHandler(stdout_handler)
        logger.info('Logging to stdout...')

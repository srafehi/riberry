import logging

from riberry import log, config, plugins, model, celery, policy, services, exc, app

__version__ = '0.10.11'

log.root_name = __name__
log.root = logging.getLogger(log.root_name)
log.root.addHandler(logging.NullHandler())

config.config.enable()
config.config.authentication.enable()

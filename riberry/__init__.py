import logging

from riberry import log

__version__ = '0.10.19'

log.root_name = __name__
log.logger = logging.getLogger(log.root_name)
log.init()

from riberry import config, plugins, model, celery, policy, services, exc, app

config.config.enable()
config.config.authentication.enable()

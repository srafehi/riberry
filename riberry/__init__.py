import logging

from riberry import log, config, plugins, model, celery, policy, services, exc, app

__version__ = '0.10.15'

log.root_name = __name__
log.logger = logging.getLogger(log.root_name)
log.init()


config.config.enable()
config.config.authentication.enable()

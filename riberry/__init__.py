from riberry import config, plugins, model, celery, policy, services, exc, app

__version__ = '0.10.10'

config.config.enable()
config.config.authentication.enable()

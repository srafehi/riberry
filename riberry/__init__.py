from riberry import config, plugins, model, celery, policy, services, exc, app

__version__ = '0.10.5'

config.config.enable()
config.config.authentication.enable()

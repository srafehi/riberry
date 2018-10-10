import riberry
import toml
import os
import binascii
import pathlib
import warnings

from riberry.util.common import variable_substitution

CONF_DEFAULT_BG_SCHED_INTERVAL = 10
CONF_DEFAULT_BG_EVENT_INTERVAL = 2
CONF_DEFAULT_BG_EVENT_PROCESS_LIMIT = 1000
CONF_DEFAULT_BG_CAPACITY_INTERVAL = 5

CONF_DEFAULT_DB_ECHO = False
CONF_DEFAULT_DB_CONN_PATH = (pathlib.Path(os.path.expanduser('~')) / '.riberry') / 'model.db'
CONF_DEFAULT_DB_CONN_URL = f'sqlite:///{CONF_DEFAULT_DB_CONN_PATH}'

CONF_DEFAULT_POLICY_PROVIDER = 'default'
CONF_DEFAULT_AUTH_PROVIDER = 'default'
CONF_DEFAULT_AUTH_TOKEN_PROVIDER = 'jwt'
CONF_DEFAULT_AUTH_TOKEN_PATH = (pathlib.Path(os.path.expanduser('~')) / '.riberry') / 'auth.key'
CONF_DEFAULT_AUTH_TOKEN_SIZE = 256


if 'RIBERRY_CONFIG_PATH' in os.environ:
    _config = variable_substitution(toml.load(os.environ['RIBERRY_CONFIG_PATH']))
else:
    warnings.warn(message=f'Environment variable \'RIBERRY_CONFIG_PATH\' not declared, '
                          f'defaulting to default configuration')
    _config = {}


def load_config_value(raw_config, default=None):
    if 'path' in raw_config:
        with open(raw_config['path']) as f:
            return f.read()
    elif 'envvar' in raw_config:
        return os.getenv(raw_config['envvar'])
    elif 'value' in raw_config:
        return raw_config['value']
    else:
        return default


class DatabaseConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        connection_config = self.raw_config.get('connection') or {}
        self.connection_url = load_config_value(connection_config)
        if not self.connection_url:
            CONF_DEFAULT_DB_CONN_PATH.parent.mkdir(exist_ok=True)
            self.connection_url = CONF_DEFAULT_DB_CONN_URL

        self.echo = connection_config.get('echo', CONF_DEFAULT_DB_ECHO)
        self.connection_arguments = self.raw_config.get('arguments', {})

    def enable(self):
        riberry.model.init(
            url=self.connection_url,
            echo=self.echo,
            connection_arguments=self.connection_arguments
        )


class AuthenticationTokenConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self.provider = self.raw_config.get('provider') or CONF_DEFAULT_AUTH_TOKEN_PROVIDER

        self.secret = load_config_value(self.raw_config)
        if not self.secret:
            self.secret = self.make_secret()

    @staticmethod
    def make_secret():
        CONF_DEFAULT_AUTH_TOKEN_PATH.parent.mkdir(exist_ok=True)
        if not CONF_DEFAULT_AUTH_TOKEN_PATH.is_file():
            with open(CONF_DEFAULT_AUTH_TOKEN_PATH, 'wb') as f:
                f.write(binascii.hexlify(os.urandom(CONF_DEFAULT_AUTH_TOKEN_SIZE)))

        with open(CONF_DEFAULT_AUTH_TOKEN_PATH, 'rb') as f:
            return f.read().decode()


class AuthenticationProviderConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self.default = self.raw_config.get('default') or CONF_DEFAULT_AUTH_PROVIDER
        self.supported = self.raw_config.get('supported') or [self.default]


class AuthenticationConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self.providers = AuthenticationProviderConfig(self.raw_config.get('providers'))
        self.token = AuthenticationTokenConfig(self.raw_config.get('token'))
        self._config_cache = {}

    def __getitem__(self, item):
        if item not in self._config_cache:
            for provider in riberry.plugins.plugin_register['authentication']:
                if provider.name() == item:
                    self._config_cache[item] = provider(self.raw_config.get(item, {}))
                    break
            else:
                raise ValueError(f'Authentication provider {item!r} not found')

        return self._config_cache[item]

    @property
    def default_provider(self):
        return self[self.providers.default]

    def enable(self):
        for provider_name in self.providers.supported:
            self[provider_name].on_enabled()


class PolicyProviderConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self.provider_name = self.raw_config.get('provider') or CONF_DEFAULT_POLICY_PROVIDER
        self._provider = None

    @property
    def provider(self):
        if self._provider is None:
            for provider in riberry.plugins.plugin_register['policies']:
                if provider.name == self.provider_name:
                    self._provider = provider
                    break
            else:
                raise ValueError(f'PolicyProviderConfig.provider:: '
                                 f'could not find register provider {self.provider_name!r}')
        return self._provider


class EmailNotificationConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self._enabled = config_dict.get('enabled', False)
        self.smtp_server = config_dict.get('smtpServer')
        self.sender = config_dict.get('sender')

    @property
    def enabled(self):
        return bool(self._enabled and self.smtp_server and self.sender)


class BackgroundTaskConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self.events = BackgroundTaskEventsConfig(self.raw_config.get('events') or {})
        self.schedules = BackgroundTaskScheduleConfig(self.raw_config.get('schedules') or {})
        self.capacity = BackgroundTaskCapacityConfig(self.raw_config.get('capacity') or {})


class BackgroundTaskEventsConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self.interval = config_dict.get('interval', CONF_DEFAULT_BG_EVENT_INTERVAL)
        self.processing_limit = config_dict.get('limit', CONF_DEFAULT_BG_EVENT_PROCESS_LIMIT)


class BackgroundTaskScheduleConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self.interval = config_dict.get('interval', CONF_DEFAULT_BG_SCHED_INTERVAL)


class BackgroundTaskCapacityConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict or {}
        self.interval = config_dict.get('interval', CONF_DEFAULT_BG_CAPACITY_INTERVAL)


class RiberryConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict
        self.authentication = AuthenticationConfig(self.raw_config.get('authentication') or {})
        self.policies = PolicyProviderConfig(self.raw_config.get('policies') or {})
        self.database = DatabaseConfig(self.raw_config.get('database') or {})
        if 'notification' in self.raw_config and isinstance(self.raw_config['notification'], dict):
            email_config = self.raw_config['notification'].get('email') or {}
        else:
            email_config = {}

        self.email = EmailNotificationConfig(email_config)
        self.background = BackgroundTaskConfig(self.raw_config.get('background') or {})

    @property
    def celery(self):
        return self.raw_config.get('celery') or {}

    def enable(self):
        self.authentication.enable()
        self.database.enable()


config: RiberryConfig = RiberryConfig(config_dict=_config)

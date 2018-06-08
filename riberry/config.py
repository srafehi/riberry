import riberry
import toml
import os


_config = toml.load(os.environ['RIBERRY_CONFIG_PATH'])


def load_config_value(config):
    if 'path' in config:
        with open(config['path']) as f:
            return f.read()
    elif 'envvar' in config:
        return os.getenv(config['envvar'])
    elif 'value' in config:
        return config['value']


class DatabaseConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict
        self.connection_string = load_config_value(self.raw_config['connection'])
        self.echo = self.raw_config['connection'].get('echo', False)
        self.connection_arguments = self.raw_config.get('arguments', {})

    def enable(self):
        riberry.model.init(
            url=self.connection_string,
            echo=self.echo,
            connection_arguments=self.connection_arguments
        )


class AuthenticationConfigToken:

    def __init__(self, config_dict):
        self.raw_config = config_dict
        self.provider = self.raw_config['provider']
        self.secret = load_config_value(self.raw_config)


class AuthenticationConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict
        self.provider_names = self.raw_config['providers']
        self.default_provider_name = self.raw_config['default']
        self.token = AuthenticationConfigToken(self.raw_config['token'])
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
        return self[self.default_provider_name]

    def enable(self):
        for provider_name in self.provider_names:
            self[provider_name].on_enabled()

    @classmethod
    def from_config(cls):
        pass


class EmailConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict
        self._enabled = config_dict.get('enabled', False)
        self.smtp_server = config_dict.get('smtpServer')
        self.sender = config_dict.get('sender')

    @property
    def enabled(self):
        return self._enabled and self.smtp_server and self.sender


class RiberryConfig:

    def __init__(self, config_dict):
        self.raw_config = config_dict
        self.authentication = AuthenticationConfig(self.raw_config['authentication'])
        self.database = DatabaseConfig(self.raw_config['database'])
        self.email = EmailConfig(self.raw_config.get('email', {}))

    def enable(self):
        self.authentication.enable()
        self.database.enable()


config: RiberryConfig = RiberryConfig(config_dict=_config)

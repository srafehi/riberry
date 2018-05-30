import abc


class AuthenticationProvider(metaclass=abc.ABCMeta):

    def __init__(self, config_dict):
        self.raw_config = config_dict

    @classmethod
    def name(cls) -> str:
        raise NotImplementedError

    def authenticate(self, username: str, password: str) -> bool:
        raise NotImplementedError

    def secure_password(self, password: bytes) -> bytes:
        raise NotImplementedError

    def on_enabled(self):
        pass

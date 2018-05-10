class DummyConfig:

    def __init__(self, authentication_provider=None):
        self.__authentication_provider = authentication_provider

    @property
    def authentication_provider(self):
        return self.__authentication_provider

    @authentication_provider.setter
    def authentication_provider(self, value):
        self.__authentication_provider = value


config = DummyConfig()

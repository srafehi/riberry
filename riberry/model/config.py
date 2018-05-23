class DummyConfig:

    def __init__(self, authentication_provider=None):
        self.authentication_provider = authentication_provider
        self.secrets = {
            'jwt_secret': 'INSERT_TOKEN_HERE'
        }


config = DummyConfig()

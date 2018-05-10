from riberry.model import base, auth, config


class DummyAuthenticationProvider:

    def authenticate(self, username, password):
        return auth.User.query().filter_by(username=username, password=password).first()


config.config.authentication_provider = DummyAuthenticationProvider()

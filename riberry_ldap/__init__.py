from riberry import plugins, background
from riberry.plugins.interfaces import AuthenticationProvider


def sample_task():
    print('Hello world!')


class LdapAuthenticationProvider(AuthenticationProvider):

    @classmethod
    def name(cls) -> str:
        return 'ldap'

    def authenticate(self, username: str, password: str) -> bool:
        raise NotImplementedError

    def secure_password(self, password: bytes) -> bytes:
        raise NotImplementedError

    def on_enabled(self):
        background.register_task('riberry_ldap:sample_task', schedule=5)


plugins.plugin_register['authentication'].add(LdapAuthenticationProvider)

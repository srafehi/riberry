from datetime import timedelta
import pendulum

from riberry.model import auth
# noinspection PyUnresolvedReferences
from tests.unit.riberry.fixtures import dummy_user, init_model


class TestAuthToken:

    def test_username(self, dummy_user: auth.User):
        token = auth.AuthToken.create(dummy_user)
        payload = auth.AuthToken.verify(token)
        assert payload['subject'] == dummy_user.username

    def test_expiry_delta(self, dummy_user: auth.User):
        expiry_delta = timedelta(hours=1)
        token = auth.AuthToken.create(dummy_user, expiry_delta=expiry_delta)
        payload = auth.AuthToken.verify(token)
        iat = pendulum.from_timestamp(payload['iat'])
        exp = pendulum.from_timestamp(payload['exp'])
        assert exp - iat == expiry_delta

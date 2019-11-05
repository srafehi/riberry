import pytest
from sqlalchemy.exc import IntegrityError

from riberry.model import auth, conn
from riberry.plugins.defaults.authentication import hash_password
# noinspection PyUnresolvedReferences
from tests.unit.riberry.fixtures import dummy_user, init_model


class TestUser:

    def test_enforce_unique_username(self):
        user_a = auth.User(username='johndoe', password=hash_password(b'password').decode())
        user_b = auth.User(username='johndoe', password=hash_password(b'password').decode())
        conn.add(user_a)
        conn.add(user_b)

        with pytest.raises(IntegrityError):
            conn.commit()

    def test_reject_blank_usernames(self):
        with pytest.raises(ValueError):
            auth.User(username=None)

    def test_reject_short_usernames(self):
        with pytest.raises(ValueError):
            auth.User(username='ab')

    @pytest.mark.usefixtures('dummy_user')
    def test_repr(self):
        user = auth.User.authenticate(username='johndoe', password='password')
        assert 'username=\'johndoe\'' in repr(user)

    @pytest.mark.usefixtures('dummy_user')
    def test_valid_password(self):
        user = auth.User.authenticate(username='johndoe', password='password')
        assert user.username == 'johndoe'

    @pytest.mark.usefixtures('dummy_user')
    def test_invalid_username(self):
        with pytest.raises(Exception):
            auth.User.authenticate(username='invalid', password='password')

    @pytest.mark.usefixtures('dummy_user')
    def test_invalid_password(self):
        with pytest.raises(Exception):
            auth.User.authenticate(username='johndoe', password='invalid')


class TestUserDetails:

    @pytest.mark.usefixtures('dummy_user')
    def test_full_name(self, dummy_user: auth.User):
        assert dummy_user.details.full_name == f'{dummy_user.details.first_name} {dummy_user.details.last_name}'

    @pytest.mark.usefixtures('dummy_user')
    def test_invalid_email_no_period(self, dummy_user: auth.User):
        with pytest.raises(ValueError):
            dummy_user.details.email = 'invalid@email'

    @pytest.mark.usefixtures('dummy_user')
    def test_invalid_email_no_ampersand(self, dummy_user: auth.User):
        with pytest.raises(ValueError):
            dummy_user.details.email = 'invalid-world'

    @pytest.mark.usefixtures('dummy_user')
    def test_valid_email(self, dummy_user: auth.User):
        dummy_user.details.email = 'hello@world.com'
        conn.commit()

        assert dummy_user.details.email == 'hello@world.com'

from riberry.model import init, conn, auth, base
from sqlalchemy.exc import IntegrityError
import pytest


@pytest.fixture(autouse=True)
def init_model():
    init(url='sqlite://')
    yield
    base.Base.metadata.drop_all(conn.raw_engine)


@pytest.fixture
def dummy_user():
    user = auth.User(username='johndoe', password='password')
    conn.add(user)
    conn.commit()


def test_user_enforce_unique_username():
    user_a = auth.User(username='johndoe', password='password')
    user_b = auth.User(username='johndoe', password='password')
    conn.add(user_a)
    conn.add(user_b)

    with pytest.raises(IntegrityError):
        conn.commit()


def test_user_reject_blank_usernames():
    with pytest.raises(ValueError):
        auth.User(username=None)


def test_user_reject_short_usernames():
    with pytest.raises(ValueError):
        auth.User(username='ab')


@pytest.mark.usefixtures('dummy_user')
def test_user_repr():
    user = auth.User.authenticate(username='johndoe', password='password')
    assert 'username=\'johndoe\'' in repr(user)


@pytest.mark.usefixtures('dummy_user')
def test_user_valid_password():
    user = auth.User.authenticate(username='johndoe', password='password')
    assert user.username == 'johndoe'


@pytest.mark.usefixtures('dummy_user')
def test_user_invalid_username():
    with pytest.raises(Exception):
        auth.User.authenticate(username='invalid', password='password')


@pytest.mark.usefixtures('dummy_user')
def test_user_invalid_password():
    with pytest.raises(Exception):
        auth.User.authenticate(username='johndoe', password='invalid')
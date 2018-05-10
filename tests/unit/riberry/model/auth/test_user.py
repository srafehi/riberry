from riberry.model import init, conn, auth, base
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
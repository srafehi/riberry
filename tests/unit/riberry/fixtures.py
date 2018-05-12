import pytest
from riberry.model import init, conn, auth, base


@pytest.fixture(autouse=True)
def init_model():
    init(url='sqlite://')
    yield
    base.Base.metadata.drop_all(conn.raw_engine)


@pytest.fixture
def dummy_user():
    user = auth.User(username='johndoe', password='password')
    user.details = auth.UserDetails(first_name='John', last_name='Doe')
    conn.add(user)
    conn.commit()
    return user
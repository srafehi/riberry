import pytest

from riberry.model import init, conn, auth, base
from riberry.plugins.defaults.authentication import hash_password


@pytest.fixture(autouse=True, scope='module')
def init_model():
    base.Base.metadata.drop_all(conn.raw_engine)
    base.Base.metadata.create_all(conn.raw_engine)
    yield
    base.Base.metadata.drop_all(conn.raw_engine)


@pytest.fixture(autouse=True)
def init_model():
    with conn:
        yield


@pytest.fixture
def dummy_user():
    with conn:
        user = auth.User(username='johndoe', password=hash_password(b'password').decode())
        user.details = auth.UserDetails(first_name='John', last_name='Doe')
        conn.add(user)
        conn.commit()
        yield user

    with conn:
        user = auth.User.query().filter_by(username='johndoe').one()
        conn.delete(user.details)
        conn.delete(user)
        conn.commit()


@pytest.fixture
def model_to_dict():
    def _model_to_dict(model):
        model_dict = vars(model)
        model_dict.pop('_sa_instance_state')
        return model_dict

    return _model_to_dict

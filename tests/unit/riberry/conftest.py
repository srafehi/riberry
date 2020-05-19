import pytest

from riberry.model import conn, auth, base
from riberry.plugins.defaults.authentication import hash_password


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
        conn.delete(user)
        conn.commit()


@pytest.fixture
def model_to_dict():
    def _model_to_dict(model):
        model_dict = vars(model)
        model_dict.pop('_sa_instance_state')
        return model_dict

    return _model_to_dict

import os
import pathlib

import pytest
import helpers


ROOT = pathlib.Path(__file__).parent
RIB_TOML = str(ROOT / 'config' / 'riberry.toml')
APP_YAML = str(ROOT / 'config' / 'apps.yaml')
CELERY_APP_PATH = str(ROOT / 'apps' / 'celery')

os.environ['RIBERRY_CONFIG_PATH'] = RIB_TOML


@pytest.fixture(autouse=True)
def session_scope():
    import riberry
    with riberry.model.conn:
        yield


@pytest.fixture(autouse=True, scope='session')
def _session_recreate_database():
    helpers.recreate_database()


@pytest.fixture
def recreate_database():
    helpers.recreate_database()


@pytest.fixture
def empty_database():
    helpers.empty_database()


@pytest.fixture(scope='session')
def rib_toml():
    return RIB_TOML


@pytest.fixture(scope='session')
def app_yaml():
    return APP_YAML


@pytest.fixture(scope='session')
def celery_app_path():
    return CELERY_APP_PATH

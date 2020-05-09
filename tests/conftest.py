import contextlib
import os
import pathlib

import pytest

ROOT = pathlib.Path(__file__).parent
RIB_TOML = str(ROOT / 'config' / 'riberry.toml')
APP_YAML = str(ROOT / 'config' / 'apps.yaml')
CELERY_APP_PATH = str(ROOT / 'apps' / 'celery')

os.environ['RIBERRY_CONFIG_PATH'] = RIB_TOML


@pytest.fixture
def recreate_database():
    from riberry import model
    model.base.Base.metadata.drop_all(model.conn.raw_engine)
    model.base.Base.metadata.create_all(model.conn.raw_engine)
    yield
    model.base.Base.metadata.drop_all(model.conn.raw_engine)


@pytest.fixture
def empty_database():
    from riberry import model
    with contextlib.closing(model.conn.raw_engine.connect()) as connection:
        transaction = connection.begin()
        for table in reversed(model.base.Base.metadata.sorted_tables):
            connection.execute(table.delete())
        transaction.commit()


@pytest.fixture(scope='session')
def rib_toml():
    return RIB_TOML


@pytest.fixture(scope='session')
def app_yaml():
    return APP_YAML


@pytest.fixture(scope='session')
def celery_app_path():
    return CELERY_APP_PATH

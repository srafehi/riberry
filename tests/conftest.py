import os
import pathlib

import pytest

ROOT = pathlib.Path(__file__).parent
RIB_TOML = str(ROOT / 'config' / 'riberry.toml')
APP_YAML = str(ROOT / 'config' / 'apps.yaml')
CELERY_APP_PATH = str(ROOT / 'apps' / 'celery')

os.environ['RIBERRY_CONFIG_PATH'] = RIB_TOML


@pytest.fixture(scope='session')
def rib_toml():
    return RIB_TOML


@pytest.fixture(scope='session')
def app_yaml():
    return APP_YAML


@pytest.fixture(scope='session')
def celery_app_path():
    return CELERY_APP_PATH

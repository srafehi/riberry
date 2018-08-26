import os
import uuid
from unittest.mock import patch

import pytest
import toml

id_ = uuid.uuid4()
PATH = os.path.abspath(f'test-rib-{id_}.toml')
DB_PATH = os.path.abspath(f'test-rib-{id_}.db')
os.environ['RIBERRY_CONFIG_PATH'] = PATH


with open(PATH, 'w') as f:
    toml.dump({
        'database': {
            'connection': {
                'value': f'sqlite:///{DB_PATH}'
            }
        },
        'notification': {
            'email': {
                'enabled': True,
                'smtpServer': 'fake.smtp.server',
                'sender': 'noreply@fake.domain.name',
            }
        }
    }, f)


@pytest.fixture(autouse=True, scope='session')
def rib_setup(request):
    def cleanup():
        os.remove(PATH)
        os.remove(DB_PATH)
    request.addfinalizer(cleanup)


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': 'redis://',
        'result_backend': 'redis://',
    }


@pytest.fixture(scope='session')
def celery_worker_parameters():
    return {
        'queues':  ('celery', 'rib.event'),
    }


@pytest.fixture(scope='session', autouse=True)
def email_notification_mock():
    with patch('riberry.celery.background.events.events.email_notification') as _fixture:
        yield _fixture


@pytest.fixture(scope='session')
def riberry_workflow(celery_session_app):
    from riberry.celery.client import Workflow

    wf = Workflow(name='Dummy', app=celery_session_app)
    return wf


@pytest.fixture(scope='session')
def execution_id(riberry_workflow, email_notification_mock, celery_session_app, celery_session_worker):
    from riberry import model
    from .helper import execute_riberry_job

    with model.conn:
        return execute_riberry_job(riberry_workflow, instance_name='instance', interface_name='int', interface_version=1)

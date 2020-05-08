import os
import pathlib
import subprocess

import pytest

RIB_TOML = str(pathlib.Path(__file__).parent.parent / 'config' / 'riberry.toml')
APP_YAML = str(pathlib.Path(__file__).parent.parent / 'config' / 'apps.yaml')
CELERY_APP_PATH = str(pathlib.Path(__file__).parent.parent / 'apps' / 'celery')

os.environ['RIBERRY_CONFIG_PATH'] = RIB_TOML


@pytest.fixture(scope="session", autouse=True)
def init_riberry():
    import riberry
    from riberry.util import config_importer, user, groups

    riberry.model.base.Base.metadata.drop_all(riberry.model.conn.raw_engine)
    riberry.model.base.Base.metadata.create_all(riberry.model.conn.raw_engine)

    config_importer.import_from_file(config_path=APP_YAML, dry_run=False)
    user.add_user(
        username='admin',
        password='password',
        first_name='Admin',
        last_name='Account',
        display_name='Admin Account',
        department='Test Department',
        email='test@riberry.app',
    )
    groups.add_user_to_group(username='admin', group_name='sysadmin')
    riberry.model.conn.commit()


@pytest.fixture(scope='module')
def start_celery_worker(request):
    def start(cmd, riberry_instance, app_path=None):
        proc = subprocess.Popen(
            cmd,
            cwd=app_path or CELERY_APP_PATH,
            env={**os.environ, **{'RIBERRY_INSTANCE': riberry_instance}},
        )
        request.addfinalizer(proc.kill)

    return start

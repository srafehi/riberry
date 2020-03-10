import json

import pytest

from riberry import testing
from base import BasicBaseTestSuite


@pytest.fixture(scope='module', autouse=True)
def start_basic_app(start_celery_worker):
    start_celery_worker(
        cmd=['celery', 'worker', '-A', 'basic', '-l', 'debug'],
        riberry_instance='test.celery.instance',
    )


class TestBasicSchemaCeleryApp(BasicBaseTestSuite):

    entry_point_step_name = 'entry_point_schema'

    @classmethod
    def setup_class(cls):
        cls.setup_test_scenario(
            job_specification=testing.base.JobSpecification(
                form_internal_name='test.celery.form.basic.schema',
                username='admin',
                input_data={
                    'streams': 2,
                }
            )
        )

    @classmethod
    def teardown_class(cls):
        cls.delete_test_job()

    def test_job_input_values(self):
        assert self.job_values == {
            'data': {
                'streams': 2,
            }
        }

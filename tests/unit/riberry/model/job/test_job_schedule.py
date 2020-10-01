from unittest import mock

import pendulum
import pytest

from riberry.model.job import JobSchedule


@pytest.mark.parametrize(['enabled', 'run_when_online', 'instance_status', 'expected_value'], [
    (False, False, 'offline', False),
    (False, True, 'online', False),
    (True, True, 'offline', False),
    (True, False, 'offline', True),
    (True, False, 'online', True),
    (True, True, 'online', True),
])
def test_active(enabled, run_when_online, instance_status, expected_value):
    job_schedule = JobSchedule(enabled=enabled, run_when_online=run_when_online)
    dummy_job = mock.MagicMock()
    dummy_job.instance.status = instance_status
    job_schedule.job = dummy_job
    assert job_schedule.active == expected_value


@pytest.mark.parametrize(['timezone', 'current_datetime', 'expected_datetime'], [
    ('UTC', '2020-04-04T02:00:00+0000', '2020-04-03T15:00:00+0000'),
    ('UTC', '2020-04-04T15:00:00+0000', '2020-04-04T15:00:00+0000'),
    ('UTC', '2020-04-05T15:00:00+0000', '2020-04-05T15:00:00+0000'),
    ('Australia/Melbourne', '2020-04-04T02:00:00+0000', '2020-04-03T15:00:00+1100'),
    ('Australia/Melbourne', '2020-04-04T15:00:00+0000', '2020-04-04T15:00:00+1100'),
    ('Australia/Melbourne', '2020-04-05T15:00:00+0000', '2020-04-05T15:00:00+1000'),
])
def test_run_with_timezone(timezone, current_datetime, expected_datetime):
    job_schedule = JobSchedule(
        enabled=True,
        run_when_online=False,
        cron='0 15 * * *',
        total_runs=0,
        created=pendulum.parse('2020-04-03T02:00:00+0000'),
        timezone=timezone,
    )
    with pendulum.test(pendulum.parse(current_datetime)):
        dummy_job = mock.MagicMock()
        job_schedule.job = dummy_job
        job_schedule.run()
        assert job_schedule.last_run == pendulum.parse(expected_datetime)
        assert job_schedule.last_run.tzname() == 'UTC'
        job_schedule.job.execute.assert_called_once()

from unittest.mock import MagicMock

import celery
import pytest

from riberry import model
from riberry.celery.client import wf


@pytest.fixture(autouse=True, scope='session')
def registered_tasks(riberry_workflow):
    re = {
        'task_a': celery.current_app.task()(stream_start),
        'task_b': celery.current_app.task()(stream_end)
    }
    riberry_workflow.entry(name='int', version=1)(make_entry_point(re))
    return


@pytest.fixture(autouse=True, scope='session')
def registered_tasks(riberry_workflow):
    re = {
        'task_a': celery.current_app.task()(stream_start),
        'task_b': celery.current_app.task()(stream_end)
    }
    riberry_workflow.entry(name='int', version=1)(make_entry_point(re))
    return


def stream_start():
    wf.send_email(
        subject='default-sender',
        body='email-body',
        mime_type='html',
        receivers=['receiver@fake.domain.name']
    )
    wf.send_email(
        subject='custom-sender',
        body='email-body',
        mime_type='html',
        sender='custom@fake.domain.name',
        receivers=['receiver@fake.domain.name']
    )


def stream_end():
    # noinspection PyTypeChecker
    wf.artifact(filename='content-as-none', content=None)
    wf.artifact(filename='content-as-blank', content='')
    wf.artifact(filename='content-as-string', content='string')
    wf.artifact(filename='content-as-bytes', content=b'bytes')


def make_entry_point(registered_tasks):

    def entry_point(task, **inputs):
        task_a = registered_tasks['task_a']
        task_b = registered_tasks['task_b']
        stream = wf.stream_start(task_a, 'CustomStream').s() | wf.stream_end(task_b, 'CustomStream').si()
        task.replace(stream)

    return entry_point


class TestRiberryIntegration:

    @staticmethod
    def execution(execution_id) -> model.job.JobExecution:
        return model.job.JobExecution.query().filter_by(id=execution_id).one()

    @classmethod
    def stream_for_execution(cls, execution_id, stream) -> model.job.JobExecutionStream:
        execution = cls.execution(execution_id=execution_id)
        return model.job.JobExecutionStream.query().filter_by(
            job_execution=execution,
            name=stream,
        ).one()

    @classmethod
    def artifact_for_execution(cls, execution_id, filename) -> model.job.JobExecutionArtifact:
        execution = cls.execution(execution_id=execution_id)
        return model.job.JobExecutionArtifact.query().filter_by(
            job_execution=execution,
            filename=filename,
        ).one()

    def test_all_events_processed(self, execution_id):
        with model.conn:
            assert not model.misc.Event.query().all()

    def test_email_notification_count(self, email_notification_mock: MagicMock):
        assert email_notification_mock.call_count == 4

    def test_email_notification_custom_email_default_sender(self, email_notification_mock: MagicMock):
        email_notification_mock.assert_any_call(
            body='email-body',
            host='fake.smtp.server',
            mime_type='html',
            recipients=['johnsmith@fake.domain.name', 'receiver@fake.domain.name'],
            sender='noreply@fake.domain.name',
            subject='default-sender',
        )

    def test_email_notification_custom_email_custom_sender(self, email_notification_mock: MagicMock):
        email_notification_mock.assert_any_call(
            body='email-body',
            host='fake.smtp.server',
            mime_type='html',
            recipients=['johnsmith@fake.domain.name', 'receiver@fake.domain.name'],
            sender='custom@fake.domain.name',
            subject='custom-sender',
        )

    def test_email_notification_started(self, email_notification_mock: MagicMock):
        email_notification_mock.assert_any_call(
            body='Processing execution #1 for job Dummy',
            host='fake.smtp.server',
            mime_type='plain',
            recipients=['johnsmith@fake.domain.name'],
            sender='noreply@fake.domain.name',
            subject='Riberry / Started / Dummy / execution #1'
        )

    def test_email_notification_completed(self, email_notification_mock: MagicMock):
        email_notification_mock.assert_any_call(
            body='Completed execution #1 for job Dummy with status success',
            host='fake.smtp.server',
            mime_type='plain',
            recipients=['johnsmith@fake.domain.name'],
            sender='noreply@fake.domain.name',
            subject='Riberry / Success / Dummy / execution #1'
        )

    def test_successful_workflow(self, execution_id):
        with model.conn:
            execution = self.execution(execution_id=execution_id)
            assert execution.status == 'SUCCESS'

    def test_successful_primary_stream(self, execution_id):
        with model.conn:
            assert self.stream_for_execution(execution_id, 'Overall').status == 'SUCCESS'

    def test_successful_custom_stream(self, execution_id):
        with model.conn:
            assert self.stream_for_execution(execution_id, 'CustomStream').status == 'SUCCESS'

    def test_artifact_content_as_none(self, execution_id):
        with model.conn:
            artifact = self.artifact_for_execution(execution_id, 'content-as-none')
            assert artifact.binary.binary is None

    def test_artifact_content_as_blank(self, execution_id):
        with model.conn:
            artifact = self.artifact_for_execution(execution_id, 'content-as-blank')
            assert artifact.binary.binary == b''

    def test_artifact_content_as_string(self, execution_id):
        with model.conn:
            artifact = self.artifact_for_execution(execution_id, 'content-as-string')
            assert artifact.binary.binary == b'string'

    def test_artifact_content_as_bytes(self, execution_id):
        with model.conn:
            artifact = self.artifact_for_execution(execution_id, 'content-as-bytes')
            assert artifact.binary.binary == b'bytes'

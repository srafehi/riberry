import uuid
from typing import List, Type, Union, Callable

import riberry
from riberry import testing
from riberry.celery.background.events.events import process as _process_events


def setup_test_scenario(
        test_app: Type[testing.base.TestApplication],
        job_specification: testing.base.JobSpecification,
):
    """ Creates a new job for the given class from the given specification. """

    form: riberry.model.interface.Form = riberry.model.interface.Form.query().filter_by(
        internal_name=job_specification.form_internal_name,
    ).one()
    testing.util.run_until_successful(
        func=testing.waiters.wait_for_instance_online,
        kwargs=dict(instance_internal_name=form.instance.internal_name),
        time_limit=30,
        timeout_assertion_message='Application instance not online in time',
    )

    job = job_specification.create()

    executions = job.executions
    assert len(executions) == 1, f'Expected 1 execution, found {len(executions)}'
    test_app.execution_id = executions[0].id
    test_app.job_id = job.id
    test_app.job_values = {input_.internal_name: input_.value for input_ in job.values}
    test_app.job_files = {input_.internal_name: input_.binary for input_ in job.files}


def verify_execution_completion(
        execution: riberry.model.job.JobExecution,
        completion_status: str = 'SUCCESS',
        process_events: bool = True,
        time_limit: int = 30,
):
    """ Waits for a job to complete with the given status.

    The specified time limit is applied twice:
        1. Waiting for the execution to complete
        2. Waiting for the execution's events to complete processing
    """

    # Wait for the execution to complete
    testing.util.run_until_successful(
        func=testing.waiters.wait_for_execution_completion,
        kwargs=dict(execution=execution, expected_completion_status=completion_status),
        time_limit=time_limit,
        timeout_assertion_message='Execution did not complete in time',
    )

    # Manually process execution's events
    if process_events:
        _process_queued_events(execution=execution)

    # Wait for the execution's events to complete processing
    testing.util.run_until_successful(
        func=testing.waiters.wait_for_execution_events_to_process,
        args=(execution,),
        time_limit=time_limit,
        timeout_assertion_message='Execution events did not complete in time',
    )


def verify_streams(execution: riberry.model.job.JobExecution):
    """ Ensures that all streams and steps are successful and "correct". """

    for stream in execution.streams:
        assert all((stream.created, stream.started, stream.completed, stream.updated)), 'Not all stream dates populated'
        assert stream.completed >= stream.started, 'Stream dates are not correct'
        assert stream.status == 'SUCCESS'
        for step in stream.steps:
            assert all((step.created, step.started, step.completed, step.updated)), 'Not all step dates populated'
            assert step.completed >= step.started, 'Step dates are not correct'


def create_job(
        form_internal_name: str,
        username: str,
        job_prefix: str = 'TEST_',
        job_name: Union[Callable[[], str], str] = uuid.uuid4,
        input_values: dict = None,
        input_files: dict = None,
        execute: bool = True,
):
    """ Creates a test job for the given form. """
    user = riberry.model.auth.User.query().filter_by(username=username).one()
    with riberry.services.policy.policy_scope(user=user):
        job = riberry.services.job.create_job(
            riberry.services.form.form_by_internal_name(internal_name=form_internal_name).id,
            name=f'{job_prefix}{job_name() if callable(job_name) else job_name}',
            input_values=input_values or {},
            input_files=input_files or {},
            execute=execute,
        )
        riberry.model.conn.commit()
        return job


def delete_jobs(form_internal_name: str, job_prefix: str = 'TEST_'):
    """ Deletes a job for the given form with the given prefix. """

    form_id = riberry.services.form.form_by_internal_name(internal_name=form_internal_name).id
    jobs: List[riberry.model.job.Job] = riberry.model.job.Job.query().filter(
        (riberry.model.job.Job.form_id == form_id) & riberry.model.job.Job.name.startswith(job_prefix)
    ).all()

    for job in jobs:
        riberry.model.conn.delete(instance=job)

    riberry.model.conn.commit()


def _process_queued_events(execution: riberry.model.job.JobExecution):
    """ Process the queued events for the given execution. """
    root_id = execution.task_id
    _process_events(query_extension=lambda q: q.filter_by(root_id=root_id))

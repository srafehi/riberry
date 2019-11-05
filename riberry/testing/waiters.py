import riberry


def wait_for_instance_online(instance_internal_name: str) -> bool:
    """ Returns true only when the given instance's status is "online". """
    with riberry.model.conn:
        instance: riberry.model.application.ApplicationInstance = riberry.model.application.ApplicationInstance.query().filter_by(
            internal_name=instance_internal_name,
        ).one()
        return instance.status == 'online'


def wait_for_execution_completion(execution: riberry.model.job.JobExecution, expected_completion_status: str) -> bool:
    """ Returns true only when the given execution's status matches the expected completion status.

    If the expected completion status doesn't match, an assertion error will be raised.
    """
    riberry.model.conn.expire(execution)
    if execution.status == expected_completion_status:
        return True

    assert not (
            (execution.status == 'SUCCESS' and expected_completion_status == 'FAILURE') or
            (execution.status == 'FAILURE' and expected_completion_status == 'SUCCESS')
    ), f'Expected state {expected_completion_status}, got {execution.status}'

    return False


def wait_for_execution_events_to_process(execution: riberry.model.job.JobExecution) -> bool:
    """ Returns true only when the given execution has no background event queued up for processing. """

    riberry.model.conn.expire(execution)
    return riberry.model.misc.Event.query().filter_by(root_id=execution.task_id).count() == 0

from riberry.celery import client
from riberry import model, policy


@policy.context.post_authorize(action='view')
def job_execution_by_id(execution_id):
    return model.job.JobExecution.query().filter_by(id=execution_id).one()


@policy.context.post_authorize(action='view')
def job_artifact_by_id(artifact_id):
    return model.job.JobExecutionArtifact.query().filter_by(id=artifact_id).one()


@policy.context.post_authorize(action='view')
def job_stream_by_id(stream_id):
    return model.job.JobExecutionStream.query().filter_by(id=stream_id).one()


@policy.context.post_authorize(action='view')
def delete_job_execution_by_id(execution_id):
    delete_job_execution(execution=job_execution_by_id(execution_id=execution_id))


@policy.context.post_authorize(action='view')
def delete_job_execution(execution):
    model.conn.delete(execution)


@policy.context.post_authorize(action='view')
def cancel_job_execution_by_id(execution_id):
    cancel_job_execution(execution=job_execution_by_id(execution_id=execution_id))


@policy.context.post_authorize(action='view')
def cancel_job_execution(execution):
    if execution.status in ('SUCCESS', 'FAILURE'):
        return

    user = policy.context.subject
    message = 'The current execution was manually cancelled by user {} ({}).'.format(
        user.username, user.details.display_name
    ).encode()

    artifact = model.job.JobExecutionArtifact(
        name=f'Workflow cancelled by user {user.username}',
        job_execution=execution,
        type=model.job.ArtifactType.error,
        category='Fatal',
        filename='fatal.log',
        size=len(message),
        binary=model.job.JobExecutionArtifactBinary(binary=message),
    )
    model.conn.add(artifact)
    client.workflow_complete(
        task_id=execution.task_id, root_id=execution.task_id, status='FAILURE', primary_stream=None)

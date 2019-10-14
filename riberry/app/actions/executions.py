import traceback
import uuid

import pendulum

import riberry
from . import notify
from .. import current_riberry_app
from ..util.events import create_event

log = riberry.log.make(__name__)


def queue_job_execution(execution: riberry.model.job.JobExecution, track_executions: bool = True):
    job = execution.job
    form = job.form

    try:
        execution.status = 'READY'
        execution.task_id = str(uuid.uuid4())

        if track_executions:
            current_riberry_app.backend.execution_tracker.track_execution(
                root_id=execution.task_id,
                app_instance=execution.job.instance,
            )

        riberry.model.conn.commit()

        execution_task_id = current_riberry_app.start(
            execution_id=execution.id,
            root_id=execution.task_id,
            form=form.internal_name,
        )

    except:
        execution.status = 'FAILURE'
        message = traceback.format_exc().encode()
        execution.artifacts.append(
            riberry.model.job.JobExecutionArtifact(
                job_execution=execution,
                name='Error on Startup',
                type='error',
                category='Fatal',
                filename='startup-error.log',
                size=len(message),
                binary=riberry.model.job.JobExecutionArtifactBinary(
                    binary=message
                )
            )
        )
        riberry.model.conn.commit()
        raise
    else:
        return execution_task_id


def execution_complete(task_id, root_id, status, stream, context):
    job: riberry.model.job.JobExecution = riberry.model.job.JobExecution.query().filter_by(task_id=root_id).first()

    if not job:
        return

    if context:
        with context.scope(root_id=root_id, task_id=root_id, task_name=None, stream=None, step=None, category=None):
            try:
                context.event_registry.call(
                    event_type=context.event_registry.types.on_completion,
                    status=status,
                )
            except:
                log.exception('Error occurred while triggering on_completion event.')
                riberry.model.conn.rollback()

    job.task_id = root_id
    job.status = status
    job.completed = job.updated = pendulum.DateTime.utcnow()
    if not job.started:
        job.started = pendulum.DateTime.utcnow()

    if stream is None:
        stream = riberry.model.job.JobExecutionStream.query().filter_by(task_id=root_id).first()
        if stream is not None:
            stream = stream.name

    if stream is not None:
        create_event(
            name='stream',
            root_id=root_id,
            task_id=root_id,
            data={
                'stream': stream,
                'state': status
            }
        )

    riberry.model.conn.commit()
    notify.workflow_complete(
        task_id=task_id,
        root_id=root_id,
        status=status,
    )


def execution_started(task_id, root_id, job_id, primary_stream):
    job: riberry.model.job.JobExecution = riberry.model.job.JobExecution.query().filter_by(
        id=job_id,
    ).one()
    job.started = job.updated = pendulum.DateTime.utcnow()
    job.status = 'ACTIVE'
    job.task_id = root_id
    riberry.model.conn.commit()

    create_event(
        name='stream',
        root_id=root_id,
        task_id=task_id,
        data={
            'stream': primary_stream,
            'state': 'ACTIVE',
        }
    )
    notify.workflow_started(task_id=task_id, root_id=root_id)

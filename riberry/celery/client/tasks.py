import base64
import json

import pendulum
from celery import shared_task, current_app
from celery.utils.log import logger
from sqlalchemy import desc, asc

from riberry import model
from riberry.celery import client
from . import tracker


def _toggle_external_task_queue(app_instance):
    active = model.job.JobExecution.query().filter_by(
        status='ACTIVE'
    ).join(model.job.Job).filter_by(
        instance=app_instance,
    ).join(model.job.JobExecutionExternalTask).filter_by(
        status='READY'
    ).count()

    operation = 'add' if active else 'cancel'
    current_app.control.broadcast('toggle_external_task_queue', reply=False, arguments={'operation': operation})


@shared_task(ignore_result=True)
def poll():
    with model.conn:
        app_instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
            internal_name=client.current_instance_name(raise_on_none=True)
        ).first()
        if not app_instance:
            return

        tracker.check_stale_execution(app_instance=app_instance)
        _toggle_external_task_queue(app_instance=app_instance)
        if app_instance.status != 'online':
            return

        executions = model.job.JobExecution.query().filter(
            model.job.JobExecution.status == 'RECEIVED'
        ).join(model.job.Job).order_by(
            desc(model.job.JobExecution.priority),
            asc(model.job.JobExecution.created),
            asc(model.job.JobExecution.id),
        ).filter_by(instance=app_instance).all()

        for execution in executions:
            task = client.queue_job_execution(execution=execution)
            logger.info(f'poll - queueing task {task}')


@shared_task(ignore_result=True)
def echo():
    with model.conn:
        app_instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
            internal_name=client.current_instance_name(raise_on_none=True)
        ).first()
        if not app_instance:
            return

        heartbeat = model.application.Heartbeat.query().filter_by(instance=app_instance).first()
        if not heartbeat:
            heartbeat = model.application.Heartbeat(instance=app_instance)
            model.conn.add(heartbeat)

        heartbeat.updated = pendulum.DateTime.utcnow()
        model.conn.commit()


def create_event(name, root_id, task_id, data=None, binary=None):
    if not root_id:
        return

    if current_app.main != 'default':
        event_call = event.delay
    else:
        event_call = event

    event_call(
        name=name,
        time=pendulum.DateTime.utcnow().timestamp(),
        root_id=root_id,
        task_id=task_id,
        data=data,
        binary=base64.b64encode(binary).decode() if binary is not None else None
    )


@shared_task(ignore_result=True)
def event(name, time, task_id, root_id, data=None, binary=None):
    with model.conn:
        evt = model.misc.Event(
            name=name,
            time=time,
            task_id=task_id,
            root_id=root_id,
            data=json.dumps(data),
            binary=base64.b64decode(binary) if binary is not None else None
        )

        model.conn.add(evt)
        model.conn.commit()


@shared_task(queue='event', ignore_result=True)
def workflow_step_update(root_id, stream_name, step_name, task_id, status=None, note=None):
    with model.conn:
        job = model.job.JobExecution.query().filter_by(task_id=root_id).one()
        stream = model.job.JobExecutionStream().query().filter_by(job_execution=job, name=stream_name).first()
        step = model.job.JobExecutionStreamStep.query().filter_by(stream=stream, task_id=task_id).first()
        if not step:
            step = model.job.JobExecutionStreamStep(name=step_name, task_id=task_id)
            stream.steps.append(step)

        if status is not None:
            step.status = status
        if note is not None:
            step.note = note

        model.conn.commit()


@shared_task(queue='event', ignore_result=True)
def workflow_stream_update(root_id, stream_name, task_id, status):
    with model.conn:
        job = model.job.JobExecution.query().filter_by(task_id=root_id).one()
        stream = model.job.JobExecutionStream.query().filter_by(job_execution=job, name=stream_name).first()
        if not stream:
            stream = model.job.JobExecutionStream(name=stream_name, task_id=task_id)
            job.streams.append(stream)

        stream.status = status
        model.conn.commit()


@shared_task(bind=True, ignore_result=True)
def workflow_complete(task, status, primary_stream):
    with model.conn:
        return client.workflow_complete(task.request.id, task.request.root_id, status, primary_stream)


def poll_external_task(self, external_task_id):
    with model.conn:
        external_task = model.job.JobExecutionExternalTask.query().filter_by(
            task_id=external_task_id,
        ).first()

        if external_task:
            if external_task.status == 'WAITING':
                raise self.retry(countdown=1)
            elif external_task.status == 'READY':
                output_data = external_task.output_data
                if isinstance(output_data, bytes):
                    output_data = output_data.decode()
                external_task.status = 'COMPLETE'
                model.conn.commit()
                return output_data

import base64
import importlib
import json
import os

import pendulum
from celery import shared_task

from riberry import model


@shared_task
def poll():
    print(model.job.Job.query().all())
    print(os.environ['RIBERRY_INSTANCE'])
    app_instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(internal_name=os.environ['RIBERRY_INSTANCE']).first()
    if not app_instance:
        return

    execution: model.job.JobExecution = model.job.JobExecution.query().filter(model.job.JobExecution.status == 'RECEIVED').join(model.job.Job).filter_by(instance=app_instance).first()
    print(execution)
    if execution:
        module_path = app_instance.application.internal_name
        module = importlib.import_module(module_path)
        print(module)
        job = execution.job
        interface = job.interface
        data = {
            **{v.definition.internal_name: v.value for v in job.values},
            **{v.definition.internal_name: v.binary.decode() for v in job.files},
        }
        try:
            task = module.workflow.start(
                execution_id=execution.id,
                input_name=interface.internal_name,
                input_version=interface.version,
                input_data=data
            )
            execution.status = 'READY'
        except:
            raise
            # execution.status = 'FAILURE'
            # import traceback
            # message = traceback.format_exc().encode()
            # execution.artefacts.append(
            #     model.JobExecutionArtefact(
            #         job_execution=execution,
            #         name='Error on Startup',
            #         filename='startup-error.txt',
            #         type='ERROR',
            #         size=len(message),
            #         binary=message
            #     )
            # )

        print(task)
        model.conn.commit()


@shared_task
def echo():
    app_instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(internal_name=os.environ["RIBERRY_INSTANCE"]).first()
    if not app_instance:
        return

    heartbeat = model.application.Heartbeat.query().filter_by(instance=app_instance).first()
    if not heartbeat:
        heartbeat = model.application.Heartbeat(instance=app_instance)
        model.conn.add(heartbeat)

    heartbeat.updated = pendulum.DateTime.utcnow()
    model.conn.commit()


def create_event(name, root_id, task_id, data=None, binary=None):
    event.delay(
        name=name,
        time=pendulum.DateTime.utcnow().timestamp(),
        root_id=root_id,
        task_id=task_id,
        data=data,
        binary=base64.b64encode(binary).decode() if binary else None
    )


@shared_task
def event(name, time, task_id, root_id, data=None, binary=None):
    evt = model.misc.Event(
        name=name,
        time=time,
        task_id=task_id,
        root_id=root_id,
        data=json.dumps(data),
        binary=base64.b64decode(binary) if binary else None
    )

    model.conn.add(evt)
    model.conn.commit()
    model.conn.close()


@shared_task(queue='event')
def workflow_step_update(root_id, stream_name, step_name, task_id, status=None, note=None):
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
    model.conn.remove()


@shared_task(queue='event')
def workflow_stream_update(root_id, stream_name, task_id, status):
    job = model.job.JobExecution.query().filter_by(task_id=root_id).one()
    stream = model.job.JobExecutionStream.query().filter_by(job_execution=job, name=stream_name).first()
    if not stream:
        stream = model.job.JobExecutionStream(name=stream_name, task_id=task_id)
        job.streams.append(stream)

    stream.status = status
    model.conn.commit()
    model.conn.remove()


@shared_task(bind=True)
def workflow_complete(task, status):
    root_id = task.request.root_id
    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=root_id).one()
    job.status = status
    job.updated = pendulum.DateTime.utcnow()
    job.task_id = root_id

    stream = model.job.JobExecutionStream.query().filter_by(job_execution=job, name='primary').one()
    stream.status = status
    stream.updated = pendulum.DateTime.utcnow()

    model.conn.commit()
    model.conn.remove()


@shared_task(bind=True)
def workflow_start(task, job_id):
    root_id = task.request.root_id

    job = model.job.JobExecution.query().filter_by(id=job_id).one()
    job.status = 'ACTIVE'
    job.task_id = root_id

    created = pendulum.DateTime.utcnow()
    stream = model.job.JobExecutionStream(job_execution=job, name='primary', task_id=task.request.id, created=created, updated=created, status='ACTIVE')
    model.conn.add(stream)

    model.conn.commit()
    model.conn.remove()

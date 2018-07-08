import base64
import json
import os
import traceback

import pendulum
from celery import shared_task
from sqlalchemy import desc, asc

from riberry import model
from riberry.celery import client


@shared_task
def poll():
    with model.conn:
        app_instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
            internal_name=os.environ['RIBERRY_INSTANCE']
        ).first()
        
        if not app_instance or app_instance.status != 'online':
            return

        execution: model.job.JobExecution = model.job.JobExecution.query().filter(
            model.job.JobExecution.status == 'RECEIVED'
        ).join(model.job.Job).order_by(
            desc(model.job.JobExecution.priority),
            asc(model.job.JobExecution.created)
        ).filter_by(instance=app_instance).first()

        if not execution:
            return

        application_name = app_instance.application.internal_name
        workflow_app = client.Workflow.__registered__[application_name]

        job = execution.job
        interface = job.interface

        try:
            task = workflow_app.start(
                execution_id=execution.id,
                input_name=interface.internal_name,
                input_version=interface.version,
                input_values={v.definition.internal_name: v.value for v in job.values},
                input_files={v.definition.internal_name: base64.b64encode(v.binary).decode() for v in job.files}
            )
            execution.status = 'READY'
        except:
            execution.status = 'FAILURE'
            message = traceback.format_exc().encode()
            execution.artifacts.append(
                model.job.JobExecutionArtifact(
                    job_execution=execution,
                    name='Error on Startup',
                    type='error',
                    category='Fatal',
                    filename='startup-error.log',
                    size=len(message),
                    binary=model.job.JobExecutionArtifactBinary(
                        binary=message
                    )
                )
            )
        else:
            print(task)
        model.conn.commit()


@shared_task
def echo():
    with model.conn:
        app_instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
            internal_name=os.environ["RIBERRY_INSTANCE"]
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
    with model.conn:
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


@shared_task(queue='event')
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


@shared_task(queue='event')
def workflow_stream_update(root_id, stream_name, task_id, status):
    with model.conn:
        job = model.job.JobExecution.query().filter_by(task_id=root_id).one()
        stream = model.job.JobExecutionStream.query().filter_by(job_execution=job, name=stream_name).first()
        if not stream:
            stream = model.job.JobExecutionStream(name=stream_name, task_id=task_id)
            job.streams.append(stream)

        stream.status = status
        model.conn.commit()


@shared_task(bind=True)
def workflow_complete(task, status, primary_stream):
    with model.conn:
        return client.workflow_complete(task, status, primary_stream)

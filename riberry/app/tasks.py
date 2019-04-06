from typing import List

import pendulum
from celery import shared_task
from sqlalchemy import desc, asc

import riberry
from riberry.app.util import execution_tracker as tracker
from . import actions, env


@shared_task(name='riberry.core.execution_complete', bind=True, ignore_result=True)
def execution_complete(task, status, stream):
    with riberry.model.conn:
        return actions.executions.execution_complete(
            task.request.id, task.request.root_id, status, stream
        )


@shared_task(name='riberry.core.heartbeat', ignore_result=True)
def echo():
    with riberry.model.conn:
        app_instance = env.get_instance_model()

        heartbeat = riberry.model.application.Heartbeat.query().filter_by(instance=app_instance).first()
        if not heartbeat:
            heartbeat = riberry.model.application.Heartbeat(instance=app_instance)
            riberry.model.conn.add(heartbeat)

        heartbeat.updated = pendulum.DateTime.utcnow()
        riberry.model.conn.commit()


@shared_task(name='riberry.core.poll', ignore_result=True)
def poll():
    with riberry.model.conn:
        app_instance = env.get_instance_model()

        tracker.check_stale_execution(app_instance=app_instance)
        if app_instance.status != 'online':
            return

        executions = riberry.model.job.JobExecution.query().filter(
            riberry.model.job.JobExecution.status == 'RECEIVED'
        ).join(riberry.model.job.Job).order_by(
            desc(riberry.model.job.JobExecution.priority),
            asc(riberry.model.job.JobExecution.created),
            asc(riberry.model.job.JobExecution.id),
        ).filter_by(instance=app_instance).all()

        for execution in executions:
            task = actions.executions.queue_job_execution(execution=execution)
            print(f'poll - queueing task {task}')


@shared_task(name='riberry.core.refresh', ignore_result=True)
def refresh():
    with riberry.model.conn:
        app_instance = env.get_instance_model()
        actions.shared_data.update_all_data_items(app_instance=app_instance)
        actions.reports.update_all_reports(app_instance=app_instance)

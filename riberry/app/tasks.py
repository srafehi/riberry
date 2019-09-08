from typing import Callable, Optional, List

import pendulum
from sqlalchemy import desc, asc

import riberry
from riberry.app.util import execution_tracker as tracker
from . import actions, env


def echo():
    with riberry.model.conn:
        app_instance = env.get_instance_model()

        heartbeat = riberry.model.application.Heartbeat.query().filter_by(instance=app_instance).first()
        if not heartbeat:
            heartbeat = riberry.model.application.Heartbeat(instance=app_instance)
            riberry.model.conn.add(heartbeat)

        heartbeat.updated = pendulum.DateTime.utcnow()
        riberry.model.conn.commit()


def poll(
        track_executions: bool = True,
        filter_func: Optional[
            Callable[[List[riberry.model.job.JobExecution]], List[riberry.model.job.JobExecution]]
        ] = None
):
    with riberry.model.conn:
        app_instance = env.get_instance_model()

        if track_executions:
            tracker.check_stale_execution(app_instance=app_instance)

        if app_instance.status != 'online':
            return

        executions: List[riberry.model.job.JobExecution] = riberry.model.job.JobExecution.query().filter(
            riberry.model.job.JobExecution.status == 'RECEIVED'
        ).join(riberry.model.job.Job).order_by(
            desc(riberry.model.job.JobExecution.priority),
            asc(riberry.model.job.JobExecution.created),
            asc(riberry.model.job.JobExecution.id),
        ).filter_by(instance=app_instance).all()

        if callable(filter_func):
            executions = filter_func(executions)

        for execution in executions:
            execution_task_id = actions.executions.queue_job_execution(
                execution=execution, track_executions=track_executions)
            print(f'poll - queueing task {execution_task_id}')


def refresh():
    with riberry.model.conn:
        app_instance = env.get_instance_model()
        actions.shared_data.update_all_data_items(app_instance=app_instance)
        actions.reports.update_all_reports(app_instance=app_instance)

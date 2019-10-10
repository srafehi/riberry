from typing import List

import riberry
from riberry.app import current_context as ctx
from ..task_queue import TaskQueue


def ready_external_tasks() -> List[riberry.model.job.JobExecutionExternalTask]:
    return riberry.model.conn.query(
        riberry.model.job.JobExecutionExternalTask
    ).filter_by(
        status='READY',
    ).join(riberry.model.job.JobExecution).filter_by(
        status='ACTIVE',
    ).join(riberry.model.job.Job).filter_by(
        instance=ctx.current.riberry_app_instance,
    ).all()


def queue_receiver_tasks(queue: TaskQueue):
    with queue.lock:
        if not queue.limit_reached():
            with riberry.model.conn:
                external_tasks = ready_external_tasks()
                while external_tasks and not queue.limit_reached():
                    queue.submit_receiver_task(external_tasks.pop())

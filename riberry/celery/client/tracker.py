from typing import List

from celery.utils.log import logger
from sqlalchemy import desc, asc

from riberry import model
from riberry.celery import util, client


def start_tracking_execution(root_id):
    r = util.celery_redis_instance()
    instance = client.current_instance_name(raise_on_none=False)
    logger.info(f'tracker: Tracking workflow {root_id!r}')
    r.sadd(f'workflow:active:{instance}', root_id)


def check_stale_execution(app_instance):
    executions: List[model.job.JobExecution] = model.job.JobExecution.query().filter(
        model.job.JobExecution.status.in_(('ACTIVE', 'READY'))
    ).join(model.job.Job).order_by(
        desc(model.job.JobExecution.priority),
        asc(model.job.JobExecution.created)
    ).filter_by(instance=app_instance).all()

    if not executions:
        return

    r = util.celery_redis_instance()
    for execution in executions:
        if not r.sismember(f'workflow:active:{app_instance.internal_name}', execution.task_id):
            logger.info(f'tracker: Identified stale workflow {execution.task_id}')

            client.workflow_complete(
                task_id=execution.task_id,
                root_id=execution.task_id,
                status='FAILURE',
                primary_stream=None
            )

            client.wf.artifact(
                name='Workflow Cancelled',
                type=model.job.ArtifactType.error,
                category='Fatal',
                filename='fatal.log',
                content=str(
                    'The current workflow\'s ID was not found within Redis and has therefore been cancelled. This '
                    'usually occurs when Redis is flushed while an execution is active.'
                ).encode(),
                task_id=execution.task_id,
                root_id=execution.task_id
            )

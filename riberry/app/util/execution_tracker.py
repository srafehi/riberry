from typing import List

from celery.utils.log import logger
from sqlalchemy import desc, asc

import riberry


def _tracker_key(value):
    return f'workflow:active:{value}'


def start_tracking_execution(root_id):
    redis = riberry.celery.util.celery_redis_instance()
    instance = riberry.app.env.get_instance_name()
    key = _tracker_key(instance)
    logger.info(f'execution_tracker: Tracking workflow {root_id!r} via key {key!r}')
    redis.sadd(key, root_id)


def check_stale_execution(app_instance):
    executions: List[riberry.model.job.JobExecution] = riberry.model.job.JobExecution.query().filter(
        riberry.model.job.JobExecution.status.in_(('ACTIVE', 'READY'))
    ).join(riberry.model.job.Job).order_by(
        desc(riberry.model.job.JobExecution.priority),
        asc(riberry.model.job.JobExecution.created)
    ).filter_by(instance=app_instance).all()

    if not executions:
        return

    redis = riberry.celery.util.celery_redis_instance()
    for execution in executions:
        if not redis.sismember(_tracker_key(app_instance.internal_name), execution.task_id):
            logger.warn(
                f'execution_tracker: Identified stale workflow. '
                f'Root ID: {execution.task_id}, Execution ID:  {execution.id}'
            )

            riberry.app.actions.artifacts.create_artifact(
                name='Workflow Cancelled',
                type=riberry.model.job.ArtifactType.error,
                category='Fatal',
                filename='fatal.log',
                content=(
                    f'The current executions\'s ID ({execution.task_id}) was not found within Redis and has '
                    f'therefore been cancelled. This usually occurs when Redis is flushed while an execution is '
                    f'in the READY or ACTIVE state.'
                ),
                task_id=execution.task_id,
                root_id=execution.task_id,
            )

            riberry.app.actions.executions.execution_complete(
                task_id=execution.task_id,
                root_id=execution.task_id,
                status='FAILURE',
                stream=None
            )

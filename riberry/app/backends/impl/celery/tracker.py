from typing import List

import riberry

log = riberry.log.make(__name__)


def _tracker_key(value):
    return f'workflow:active:{value}'


class CeleryExecutionTracker(riberry.app.backends.tracker.RiberryExecutionTracker):

    def _check_stale_executions(
            self,
            executions: List[riberry.model.job.JobExecution],
            app_instance: riberry.model.application.ApplicationInstance
    ):
        redis = riberry.celery.util.celery_redis_instance()
        for execution in executions:
            if not redis.sismember(_tracker_key(app_instance.internal_name), execution.task_id):
                self._cancel_execution(execution=execution)

    def track_execution(self, root_id: str, app_instance: riberry.model.application.ApplicationInstance):
        redis = riberry.celery.util.celery_redis_instance()
        instance = riberry.app.env.get_instance_name()
        key = _tracker_key(instance)
        log.debug(f'Tracking execution: root={root_id!r}, key={key!r}')
        redis.sadd(key, root_id)

    def _artifact_message(self, execution: riberry.model.job.JobExecution):
        return (
            f'The current executions\'s ID ({execution.task_id}) was not found within Redis and has '
            f'therefore been cancelled. This usually occurs when Redis is flushed while an execution is '
            f'in the READY or ACTIVE state.'
        )

from typing import List

import riberry

log = riberry.log.make(__name__)


class PoolExecutionTracker(riberry.app.backends.RiberryExecutionTracker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracked_executions = set()

    def _check_stale_executions(
            self,
            executions: List[riberry.model.job.JobExecution],
            app_instance: riberry.model.application.ApplicationInstance
    ):
        for execution in executions:
            if execution.task_id not in self.tracked_executions and not self.has_pending_tasks(execution=execution):
                self._cancel_execution(execution=execution)

    def track_execution(self, root_id: str, app_instance: riberry.model.application.ApplicationInstance):
        if root_id not in self.tracked_executions:
            log.debug(f'Tracking execution: root={root_id!r}')
            self.tracked_executions.add(root_id)

    def _artifact_message(self, execution: riberry.model.job.JobExecution):
        return (
            f'The current executions\'s ID ({execution.task_id}) is no longer being tracked and has '
            f'therefore been cancelled. This usually occurs when the worker has terminated while a '
            f'task was either internally queued for execution or actively executing.'
        )

    @staticmethod
    def has_pending_tasks(execution: riberry.model.job.JobExecution):
        for task in execution.external_tasks:
            if task.status != 'COMPLETE':
                return True
        return False

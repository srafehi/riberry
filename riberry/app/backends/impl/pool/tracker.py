from typing import List

import riberry

log = riberry.log.make(__name__)
_executions_tracked = set()


class PoolExecutionTracker(riberry.app.backends.RiberryExecutionTracker):

    def _check_stale_executions(
            self,
            executions: List[riberry.model.job.JobExecution],
            app_instance: riberry.model.application.ApplicationInstance
    ):
        for execution in executions:
            if execution.task_id not in _executions_tracked:
                self._cancel_execution(execution=execution)

    def track_execution(self, root_id: str, app_instance: riberry.model.application.ApplicationInstance):
        log.debug(f'Tracking execution: root={root_id!r}')
        _executions_tracked.add(root_id)

    def _artifact_message(self, execution: riberry.model.job.JobExecution):
        return (
            f'The current executions\'s ID ({execution.task_id}) is no longer being tracked and has '
            f'therefore been cancelled. This usually occurs when the worker has terminated while a '
            f'task was either internally queued for execution or actively executing.'
        )

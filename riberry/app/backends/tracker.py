from typing import List

from sqlalchemy import desc, asc

import riberry

log = riberry.log.make(__name__)


class RiberryExecutionTracker:

    def __init__(self, backend):
        self.backend: riberry.app.backends.RiberryApplicationBackend = backend

    def check_stale_executions(self, app_instance: riberry.model.application.ApplicationInstance):
        executions: List[riberry.model.job.JobExecution] = riberry.model.job.JobExecution.query().filter(
            riberry.model.job.JobExecution.status.in_(('ACTIVE', 'READY'))
        ).join(riberry.model.job.Job).order_by(
            desc(riberry.model.job.JobExecution.priority),
            asc(riberry.model.job.JobExecution.created)
        ).filter_by(instance=app_instance).all()

        if not executions:
            return

        self._check_stale_executions(executions=executions, app_instance=app_instance)

    def _check_stale_executions(
            self,
            executions: List[riberry.model.job.JobExecution],
            app_instance: riberry.model.application.ApplicationInstance
    ):
        raise NotImplementedError

    def track_execution(self, root_id: str, app_instance: riberry.model.application.ApplicationInstance):
        raise NotImplementedError

    def _artifact_message(self, execution: riberry.model.job.JobExecution):
        raise NotImplementedError

    def _cancel_execution(self, execution: riberry.model.job.JobExecution):
        log.warning(
            f'Cancelling stale execution: id={execution.id}, root={execution.task_id!r}, job={execution.job.name!r}')

        riberry.app.actions.artifacts.create_artifact(
            name='Workflow Cancelled',
            type=riberry.model.job.ArtifactType.error,
            category='Fatal',
            filename='fatal.log',
            content=self._artifact_message(execution),
            task_id=execution.task_id,
            root_id=execution.task_id,
        )

        riberry.app.actions.executions.execution_complete(
            task_id=execution.task_id,
            root_id=execution.task_id,
            status='FAILURE',
            stream=None,
            context=riberry.app.current_context,
        )

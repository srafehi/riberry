import threading
import uuid
from contextlib import contextmanager
from typing import Optional

import riberry


class ContextCurrent:
    _state = threading.local()

    def __init__(self, context):
        self.context: riberry.app.context.Context = context
        self._worker_uuid = str(uuid.uuid4())

    def _get_state(self, key, default=None):
        _data = getattr(self._state, 'state', {})
        return _data.get(key, default)

    def _set_state(self, **state):
        self._state.state = state

    @property
    def WORKER_UUID(self):
        return self._worker_uuid

    @property
    def stream(self):
        return self._get_state('stream')

    @property
    def step(self):
        return self._get_state('step')

    @property
    def category(self):
        return self._get_state('category')

    @property
    def backend(self) -> 'riberry.app.backends.RiberryApplicationBackend':
        return self.riberry_app.backend

    @property
    def task(self):
        return self.backend.active_task()

    @property
    def riberry_app(self) -> 'riberry.app.RiberryApplication':
        return riberry.app.current_riberry_app

    @property
    def riberry_app_instance(self) -> riberry.model.application.ApplicationInstance:
        return riberry.app.env.get_instance_model()

    @property
    def task_id(self):
        return self._get_state('task_id')

    @property
    def task_name(self):
        return self._get_state('task_name')

    @property
    def root_id(self):
        return self._get_state('root_id')

    @property
    def job_execution(self) -> Optional[riberry.model.job.JobExecution]:
        return riberry.model.job.JobExecution.query().filter_by(task_id=self.root_id).first()

    @property
    def job(self) -> Optional[riberry.model.job.Job]:
        job_execution = self.job_execution
        return job_execution.job if self.job_execution else None

    @contextmanager
    def scope(self, root_id, task_id, task_name, stream, category, step):
        try:
            self._set_state(
                root_id=root_id, task_name=task_name, task_id=task_id, step=step, stream=stream, category=category)
            yield
        finally:
            self._set_state()

    @property
    def progress(self) -> str:
        progress: riberry.model.job.JobExecutionProgress = riberry.model.job.JobExecutionProgress.query().filter_by(
            job_execution=self.job_execution,
        ).order_by(
            riberry.model.job.JobExecutionProgress.id.desc(),
        ).limit(
            1
        ).first()

        return progress.message if progress else None

    @progress.setter
    def progress(self, message: str):
        if message == self.progress:
            return

        progress = riberry.model.job.JobExecutionProgress(
            job_execution=self.job_execution,
            message=message,
        )

        riberry.model.conn.add(progress)
        riberry.model.conn.commit()

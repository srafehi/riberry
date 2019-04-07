import celery
from celery import shared_task

import riberry


class CeleryBackend(riberry.app.backends.RiberryApplicationBackend):
    instance: celery.Celery

    def register_task(self, func, **kwargs) -> celery.Task:
        return self.instance.task(**kwargs)(func)

    def task_by_name(self, name: str):
        return self.instance.tasks[name]

    def start_execution(self, execution_id, root_id, entry_point) -> str:
        body = self.task_by_name(riberry.app.RiberryApplication.ENTRY_POINT_TASK_NAME).si(
            execution_id=execution_id,
            form=entry_point.form
        )

        callback_success = execution_complete.si(status='SUCCESS', stream=entry_point.stream)
        callback_failure = execution_complete.si(status='FAILURE', stream=entry_point.stream)

        body.options['root_id'] = root_id
        callback_success.options['root_id'] = root_id
        callback_failure.options['root_id'] = root_id

        signature = body.on_error(callback_failure) | callback_success
        signature.options['root_id'] = root_id

        return signature.apply_async().id

    def create_receiver_task(self, external_task_id, validator):
        return self.task_by_name(riberry.app.RiberryApplication.CHECK_EXTERNAL_TASK_NAME).si(
            external_task_id=external_task_id,
            validator=validator,
        )

    def active_task(self):
        return celery.current_task


@shared_task(name='riberry.core.execution_complete', bind=True, ignore_result=True)
def execution_complete(task, status, stream):
    with riberry.model.conn:
        return riberry.app.actions.executions.execution_complete(
            task_id=task.request.id, root_id=task.request.root_id, status=status, stream=stream
        )

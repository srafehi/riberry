from typing import Dict, AnyStr

import celery

import riberry
from . import patch, tasks, addons
from .executor import TaskExecutor


def send_task_process_rib_kwargs(self, *args, **kwargs):
    riberry_properties = {}
    if kwargs.get('kwargs'):
        for key, value in kwargs['kwargs'].items():
            if key.startswith('__rib_'):
                riberry_properties[key.replace('__rib_', '', 1)] = value


class CeleryBackend(riberry.app.backends.RiberryApplicationBackend):
    instance: celery.Celery

    ENTRY_POINT_TASK_NAME = 'riberry.core.app.entry_point'
    CHECK_EXTERNAL_TASK_NAME = 'riberry.core.app.check_external_task'

    def __init__(self, instance):
        super().__init__(instance=instance)
        self.executor = TaskExecutor()

    def initialize(self):
        patch.patch_send_task(instance=self.instance, func=send_task_process_rib_kwargs)

        # Register "entry point" task
        self.task(
            name=self.ENTRY_POINT_TASK_NAME,
        )(self.executor.entry_point_executor())

        # Register "external task checker" task
        self.task(
            name=self.CHECK_EXTERNAL_TASK_NAME,
            max_retries=None,
        )(self.executor.external_task_executor())

    def default_addons(self) -> Dict[AnyStr, 'riberry.app.addons.Addon']:
        return {
            'scale': addons.Scale(),
            'background': addons.BackgroundTasks(),
            'external-receiver': addons.ExternalTaskReceiver(),
        }

    def register_task(self, func, **options) -> celery.Task:
        wrapped_func, options = self.executor.riberry_task_executor_wrapper(func=func, task_options=options)
        return self.instance.task(**options)(wrapped_func)

    def task_by_name(self, name: AnyStr):
        return self.instance.tasks[name]

    def start_execution(self, execution_id, root_id, entry_point) -> AnyStr:
        task = self.task_by_name(self.ENTRY_POINT_TASK_NAME)
        task_signature = task.si(execution_id=execution_id, form=entry_point.form)

        callback_success = tasks.execution_complete.si(status='SUCCESS', stream=entry_point.stream)
        callback_failure = tasks.execution_complete.si(status='FAILURE', stream=entry_point.stream)

        task_signature.options['root_id'] = root_id
        callback_success.options['root_id'] = root_id
        callback_failure.options['root_id'] = root_id

        exec_signature = task_signature.on_error(callback_failure) | callback_success
        exec_signature.options['root_id'] = root_id

        riberry.app.util.events.create_event(
            name='stream',
            root_id=root_id,
            task_id=root_id,
            data={
                'stream': entry_point.stream,
                'state': 'QUEUED',
            }
        )

        return exec_signature.apply_async().id

    def create_receiver_task(self, external_task_id, validator):
        return self.task_by_name(self.CHECK_EXTERNAL_TASK_NAME).si(
            external_task_id=external_task_id,
            validator=validator,
        )

    def active_task(self):
        return celery.current_task

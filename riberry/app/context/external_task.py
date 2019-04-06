import inspect
from typing import Optional

import riberry


class ExternalTask:

    def __init__(self, context):
        self.context: riberry.app.context.Context = context

    def create(
            self,
            name: str = None,
            type: str = 'external',
            external_task_id: Optional[str] = None,
            input_data: Optional[bytes] = None
    ):
        external_task = riberry.app.actions.external_task.create_external_task(
            job_execution=self.context.current.job_execution,
            name=name,
            type=type,
            external_task_id=external_task_id,
            input_data=input_data,
        )
        return ExternalTaskCreationResult(context=self.context, instance=external_task)

    def create_receiver_task(self, external_task_id, validator):
        if inspect.isfunction(validator):
            validator = riberry.app.util.misc.function_path(validator)

        return self.context.current.riberry_app.backend.create_receiver_task(
            external_task_id=external_task_id,
            validator=validator,
        )

    @staticmethod
    def mark_as_ready(external_task_id, output_data):
        return riberry.app.actions.external_task.mark_as_ready(
            external_task_id=external_task_id,
            output_data=output_data
        )


class ExternalTaskCreationResult:

    def __init__(self, context, instance):
        self.context: riberry.app.context.Context = context
        self.instance = instance
        self.task_id = instance.task_id

    def create_receiver_task(self, validator=None):
        return self.context.external_task.create_receiver_task(external_task_id=self.task_id, validator=validator)

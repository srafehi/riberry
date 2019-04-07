import riberry
from celery import shared_task


@shared_task(name='riberry.core.execution_complete', bind=True, ignore_result=True)
def execution_complete(task, status, stream):
    with riberry.model.conn:
        return riberry.app.actions.executions.execution_complete(
            task_id=task.request.id, root_id=task.request.root_id, status=status, stream=stream
        )

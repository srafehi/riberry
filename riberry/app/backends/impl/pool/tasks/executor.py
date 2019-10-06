import threading
from contextlib import contextmanager
from queue import Empty

import time

import riberry
from riberry.app.misc.signals import task_prerun, task_postrun
from ..exc import Defer
from ..task_queue import TaskQueue, Task

log = riberry.log.make(__name__)


def execution_listener(task_queue: TaskQueue):
    try:
        task: Task = task_queue.queue.get(timeout=2.0)
    except Empty:
        return

    threading.Thread(name=f'{task.execution_id}:{task.id}:{task.definition.name}', target=execute, args=(task, task_queue)).start()


def execute(task: Task, task_queue: TaskQueue):
    try:

        job_execution: riberry.model.job.JobExecution = riberry.model.conn.query(
            riberry.model.job.JobExecution
        ).filter_by(
            id=task.execution_id,
        ).one()

        if job_execution.task_id == task.id:
            task_scope = execute_entry_task(job_execution=job_execution, task=task)
        else:
            task_scope = execute_receiver_task(job_execution=job_execution, task=task)

        context_scope = riberry.app.current_context.scope(
            root_id=job_execution.task_id,
            task_id=task.id,
            task_name=task.definition.name,
            stream=task.definition.stream,
            category=None,
            step=task.definition.step,
        )

        with context_scope, task_scope:
            execute_task(job_execution=job_execution, task=task)

    finally:
        task_queue.counter.decrement()


@contextmanager
def execute_entry_task(job_execution: riberry.model.job.JobExecution, task: Task):
    riberry.app.actions.executions.execution_started(
        task_id=task.id,
        root_id=job_execution.task_id,
        job_id=task.execution_id,
        primary_stream=task.definition.stream,
    )

    yield


# noinspection PyUnusedLocal
@contextmanager
def execute_receiver_task(job_execution: riberry.model.job.JobExecution, task: Task):
    yield


def execute_task(job_execution: riberry.model.job.JobExecution, task: Task):
    task_prerun(context=riberry.app.current_context, props={})
    riberry.app.current_riberry_app.backend.set_active_task(task=task)

    status = 'SUCCESS'
    start_time = time.time()
    try:
        task.definition.func()

    except Defer:
        status = None

    except Exception as exc:
        status = 'FAILURE'
        riberry.app.actions.artifacts.create_artifact_from_traceback(category='Fatal')
        log.exception('Failed with exception: %s', exc)

    finally:
        end_time = time.time()
        log.info('Completed in %.4f seconds', end_time - start_time)

        if status:
            riberry.app.actions.executions.execution_complete(
                task_id=task.id,
                root_id=job_execution.task_id,
                status=status,
                stream=task.definition.stream,
                context=riberry.app.current_context,
            )

        task_postrun(context=riberry.app.current_context, props={}, state=status or 'IGNORED')
        riberry.app.current_riberry_app.backend.set_active_task(task=None)

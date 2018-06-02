import functools
import traceback

import pendulum
from celery import Celery
from celery import current_task
from celery import exceptions as celery_exc
from celery.result import AsyncResult

from riberry import model
from . import tasks, wf, signals

IGNORE_EXCEPTIONS = (
    celery_exc.Ignore,
    celery_exc.Retry,
    celery_exc.SoftTimeLimitExceeded,
    celery_exc.TimeLimitExceeded
)

BYPASS_ARGS = (
    '__ss__', '__se__', '__sb__'
)


def workflow_complete(task, status):
    root_id = task.request.root_id
    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=root_id).one()
    job.task_id = root_id
    job.status = status
    job.completed = job.updated = pendulum.DateTime.utcnow()

    tasks.create_event(
        name='stream',
        root_id=root_id,
        task_id=root_id,
        data={
            'stream': 'Primary',
            'state': status
        }
    )

    model.conn.commit()
    model.conn.close()


def is_workflow_complete(task):
    root_id = task.request.root_id
    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=root_id).first()
    return job.status == 'FAILURE' if job else False


def workflow_started(task, job_id):
    root_id = task.request.root_id

    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(id=job_id).one()
    job.started = job.updated = pendulum.DateTime.utcnow()
    job.status = 'ACTIVE'
    job.task_id = root_id

    tasks.create_event(
        name='stream',
        root_id=root_id,
        task_id=root_id,
        data={
            'stream': 'Primary',
            'state': 'ACTIVE'
        }
    )
    task.stream = 'Primary'

    model.conn.commit()
    model.conn.close()


def execute_task(func, func_args, func_kwargs, task_kwargs):
    # noinspection PyBroadException
    try:
        return func(*func_args, **func_kwargs)
    except IGNORE_EXCEPTIONS:
        raise
    except:
        wf.artifact(
            name=f'Exception {current_task.name}',
            type='ERROR_HANDLED' if 'rib_fallback' in task_kwargs else 'ERROR_FATAL',
            filename=f'{current_task.name}-{current_task.request.id}.log',
            content=traceback.format_exc().encode()
        )

        if 'rib_fallback' in task_kwargs:
            fallback = task_kwargs.get('wf_fallback')
            return fallback() if callable(fallback) else fallback
        else:
            workflow_complete(current_task, status='FAILURE')
            raise


def bypass(func, **task_kwargs):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        if is_workflow_complete(current_task):
            AsyncResult(current_task.request.id).revoke()
            raise Exception('Workflow already cancelled')

        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in BYPASS_ARGS}
        return execute_task(
            func=func,
            func_args=args,
            func_kwargs=filtered_kwargs,
            task_kwargs=task_kwargs
        )

    return inner


def patch_app(app):
    def task_deco(*args, **kwargs):
        if len(args) == 1:
            if callable(args[0]):
                return Celery.task(app, bypass(*args, **kwargs), **kwargs)
            raise TypeError('argument 1 to @task() must be a callable')

        def inner(func):
            return Celery.task(app, **kwargs)(bypass(func, **kwargs))

        return inner

    app.task = task_deco
    return app


class Workflow:
    __registered__ = {}

    def __init__(self, name, app, beat_queue):
        self.__registered__[name] = self
        self.app = patch_app(app)
        self.form_entries = {}
        self.entry_point = self._make_entry_point(self.app, self.form_entries)
        self._configure_beat_queues(app, beat_queue)

    @staticmethod
    def _configure_beat_queues(app, beat_queue):
        schedule = {
            'poll-executions': {
                'task': 'riberry.client.workflow.tasks.poll',
                'schedule': 0.5,
                'options': {'queue': beat_queue}
            },
            'echo-status': {
                'task': 'riberry.client.workflow.tasks.echo',
                'schedule': 2,
                'options': {'queue': beat_queue}
            }
        }

        if not app.conf.beat_schedule:
            app.conf.beat_schedule = {}
        app.conf.beat_schedule.update(schedule)

    @staticmethod
    def _make_entry_point(app, form_entries):
        @app.task(bind=True)
        def entry_point(task, execution_id, name, version, values, files):
            workflow_started(task, execution_id)
            form_entries[(name, version)](task, **values, **files)

        return entry_point

    def entry(self, name, version):
        def wrapper(func):
            self.form_entries[(name, version)] = func

        return wrapper

    def start(self, execution_id, input_name, input_version, input_values, input_files):
        body = self.entry_point.si(
            execution_id=execution_id,
            name=input_name,
            version=input_version,
            values=input_values,
            files=input_files
        )

        callback = tasks.workflow_complete.si(status='SUCCESS')

        task = body.on_error(tasks.workflow_complete.si(status='FAILURE')) | callback
        return task()

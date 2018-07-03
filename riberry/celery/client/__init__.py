import functools
import traceback
from typing import Dict, Tuple

import pendulum
from celery import Celery
from celery import current_task
from celery import exceptions as celery_exc
from celery.result import AsyncResult

from riberry import model
from riberry.celery.client import tasks
from riberry.celery.client.dynamic import DynamicParameters
from . import wf, signals, scale, dynamic

IGNORE_EXCEPTIONS = (
    celery_exc.Ignore,
    celery_exc.Retry,
    celery_exc.SoftTimeLimitExceeded,
    celery_exc.TimeLimitExceeded
)

BYPASS_ARGS = (
    '__ss__', '__se__', '__sb__'
)


def workflow_complete(task, status, primary_stream):
    root_id = task.request.root_id
    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=root_id).one()
    job.task_id = root_id
    job.status = status
    job.completed = job.updated = pendulum.DateTime.utcnow()

    if primary_stream is None:
        stream = model.job.JobExecutionStream.query().filter_by(task_id=root_id).first()
        if stream is not None:
            primary_stream = stream.name

    if primary_stream is not None:
        tasks.create_event(
            name='stream',
            root_id=root_id,
            task_id=root_id,
            data={
                'stream': primary_stream,
                'state': status
            }
        )

    model.conn.commit()
    wf.notify(notification_type='workflow_complete', data=dict(status=status))


def is_workflow_complete(task):
    root_id = task.request.root_id
    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=root_id).first()
    return job.status == 'FAILURE' if job else False


def workflow_started(task, job_id, primary_stream):
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
            'stream': primary_stream,
            'state': 'ACTIVE'
        }
    )
    task.stream = primary_stream

    model.conn.commit()
    wf.notify(notification_type='workflow_started')


def execute_task(func, func_args, func_kwargs, task_kwargs):
    # noinspection PyBroadException
    try:
        return func(*func_args, **func_kwargs)
    except IGNORE_EXCEPTIONS:
        raise
    except:
        wf.artifact(
            name=f'Exception {current_task.name}',
            type='error',
            category='Intercepted' if 'rib_fallback' in task_kwargs else 'Fatal',
            filename=f'{current_task.name}-{current_task.request.id}.log',
            content=traceback.format_exc().encode()
        )

        if 'rib_fallback' in task_kwargs:
            fallback = task_kwargs.get('rib_fallback')
            return fallback() if callable(fallback) else fallback
        else:
            workflow_complete(current_task, status='FAILURE', primary_stream=None)
            raise


def bypass(func, **task_kwargs):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        with model.conn:
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


def patch_task(task):
    def stream_start(stream):
        return wf.s(task, stream=stream)

    def stream_end(stream):
        return wf.e(task, stream=stream)

    def step(step, stream=None):
        return wf.b(task, step=step, stream=stream)

    task.stream_start = stream_start
    task.stream_end = stream_end
    task.step = step

    return task


def patch_app(app):
    def task_deco(*args, **kwargs):
        if len(args) == 1:
            if callable(args[0]):
                return patch_task(Celery.task(app, bypass(*args, **kwargs), **kwargs))
            raise TypeError('argument 1 to @task() must be a callable')

        def inner(func):
            return patch_task(Celery.task(app, **kwargs)(bypass(func, **kwargs)))

        return inner

    app.task = task_deco
    return app


class WorkflowEntry:

    def __init__(self, name, version, func, primary_stream):
        self.name = name
        self.version = version
        self.func = func
        self.primary_stream = primary_stream


class Workflow:
    __registered__ = {}

    def __init__(self, name, app, beat_queue, dynamic_parameters=None):
        self.name = name
        self.beat_queue = beat_queue
        self.__registered__[name] = self
        self.app = patch_app(app)
        self.form_entries: Dict[Tuple[str, str], WorkflowEntry] = {}
        self.entry_point = self._make_entry_point(self.app, self.form_entries)
        self._configure_beat_queues(app, self.beat_queue)
        self.dynamic_parameters = DynamicParameters(
            riberry_workflow=self,
            handlers=dynamic_parameters,
            beat_queue=self.beat_queue
        )

    @staticmethod
    def _configure_beat_queues(app, beat_queue):
        schedule = {
            'poll-executions': {
                'task': 'riberry.celery.client.tasks.poll',
                'schedule': 2,
                'options': {'queue': beat_queue}
            },
            'echo-status': {
                'task': 'riberry.celery.client.tasks.echo',
                'schedule': 2,
                'options': {'queue': beat_queue}
            }
        }

        if not app.conf.beat_schedule:
            app.conf.beat_schedule = {}
        app.conf.beat_schedule.update(schedule)

    @staticmethod
    def _make_entry_point(app, form_entries: Dict[Tuple[str, str], WorkflowEntry]):
        @app.task(bind=True)
        def entry_point(task, execution_id, name: str, version: str, values: Dict, files: Dict):
            with model.conn:
                workflow_entry = form_entries[(name, version)]
                workflow_started(task, execution_id, workflow_entry.primary_stream)
                workflow_entry.func(task, **values, **files)

        return entry_point

    def entry(self, name, version, primary_stream='Overall'):
        def wrapper(func):
            self.form_entries[(name, version)] = WorkflowEntry(
                name=name,
                version=version,
                func=func,
                primary_stream=primary_stream)

        return wrapper

    def start(self, execution_id, input_name, input_version, input_values, input_files):
        if (input_name, input_version) not in self.form_entries:
            raise ValueError(f'Application {self.name:!r} does not have an entry point with '
                             f'name {input_name!r} and version {input_version} registered.')

        workflow_entry: WorkflowEntry = self.form_entries[(input_name, input_version)]

        body = self.entry_point.si(
            execution_id=execution_id,
            name=input_name,
            version=input_version,
            values=input_values,
            files=input_files
        )

        callback_success = tasks.workflow_complete.si(status='SUCCESS', primary_stream=workflow_entry.primary_stream)
        callback_failure = tasks.workflow_complete.si(status='FAILURE', primary_stream=workflow_entry.primary_stream)
        task = body.on_error(callback_failure) | callback_success

        return task()

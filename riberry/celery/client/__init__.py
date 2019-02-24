import base64
import functools
import os
import traceback
from typing import Dict, Tuple

import pendulum
from celery import Celery, bootsteps
from celery import current_task
from celery import exceptions as celery_exc
from celery.result import AsyncResult

from riberry import model
from riberry.celery.client import tasks
from riberry.celery.client.dynamic import DynamicParameters
from . import wf, signals, scale, dynamic, tracker

IGNORE_EXCEPTIONS = (
    celery_exc.Ignore,
    celery_exc.Retry,
    celery_exc.SoftTimeLimitExceeded,
    celery_exc.TimeLimitExceeded
)

BYPASS_ARGS = (
    '__ss__', '__se__', '__sb__'
)


def current_instance_name(raise_on_none=False) -> str:
    name = os.getenv('RIBERRY_INSTANCE')
    if name is None and raise_on_none:
        raise EnvironmentError("Environment variable 'RIBERRY_INSTANCE' not set")
    return name


def is_current_instance(instance_name: str) -> bool:
    return bool(instance_name) and current_instance_name(raise_on_none=False) == instance_name


def queue_job_execution(execution: model.job.JobExecution):

    job = execution.job
    interface = job.interface
    app_instance = job.instance

    application_name = app_instance.application.internal_name
    workflow_app = Workflow.by_internal_name(internal_name=application_name)

    try:
        execution.status = 'READY'
        execution.task_id = current_task.request.root_id
        model.conn.commit()

        task = workflow_app.start(
            execution_id=execution.id,
            input_name=interface.internal_name,
            input_version=interface.version,
            input_values={v.definition.internal_name: v.value for v in job.values},
            input_files={v.definition.internal_name: base64.b64encode(v.binary).decode() for v in job.files}
        )
        tracker.start_tracking_execution(root_id=execution.task_id)
    except:
        execution.status = 'FAILURE'
        message = traceback.format_exc().encode()
        execution.artifacts.append(
            model.job.JobExecutionArtifact(
                job_execution=execution,
                name='Error on Startup',
                type='error',
                category='Fatal',
                filename='startup-error.log',
                size=len(message),
                binary=model.job.JobExecutionArtifactBinary(
                    binary=message
                )
            )
        )
        model.conn.commit()
        raise
    else:
        return task


def workflow_complete(task_id, root_id, status, primary_stream):

    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=root_id).first()
    if not job:
        return
    
    job.task_id = root_id
    job.status = status
    job.completed = job.updated = pendulum.DateTime.utcnow()
    if not job.started:
        job.started = pendulum.DateTime.utcnow()

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
    wf.notify(
        notification_type='workflow_complete',
        data=dict(status=status),
        task_id=task_id,
        root_id=root_id
    )


def is_workflow_complete(task):
    root_id = task.request.root_id
    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=root_id).first()
    return job.status in ('FAILURE', 'SUCCESS') if job else True


def workflow_started(task, job_id, primary_stream):
    root_id = task.request.root_id

    job: model.job.JobExecution = model.job.JobExecution.query().filter_by(id=job_id).one()
    job.started = job.updated = pendulum.DateTime.utcnow()
    job.status = 'ACTIVE'
    job.task_id = root_id
    task.stream = primary_stream
    model.conn.commit()

    tasks.create_event(
        name='stream',
        root_id=root_id,
        task_id=root_id,
        data={
            'stream': primary_stream,
            'state': 'ACTIVE'
        }
    )

    wf.notify(notification_type='workflow_started')


def execute_task(func, func_args, func_kwargs, task_kwargs):
    # noinspection PyBroadException
    try:
        return func(*func_args, **func_kwargs)
    except tuple(list(IGNORE_EXCEPTIONS) + task_kwargs.get('autoretry_for', [])):
        raise
    except Exception as exc:
        wf.artifact_from_traceback(category='Intercepted' if 'rib_fallback' in task_kwargs else 'Fatal')

        if 'rib_fallback' in task_kwargs:
            fallback = task_kwargs.get('rib_fallback')
            return fallback() if callable(fallback) else fallback
        else:
            workflow_complete(current_task.request.id, current_task.request.root_id, status='FAILURE', primary_stream=None)
            raise


def bypass(func, **task_kwargs):
    @functools.wraps(func)
    def inner(*args, **kwargs):

        if not task_kwargs.get('rib_task', True):
            return func(*args, **kwargs)

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

    def __init__(self, name, app, beat_queue=None, event_queue=None, scalable_queues=None, dynamic_parameters=None):
        self.name = name
        self.beat_queue = beat_queue or 'rib.beat'
        self.event_queue = event_queue or 'rib.event'
        self.__registered__[name] = self
        self.app = patch_app(app)
        self.scale = scale.ConcurrencyScale(self.app, target_queues=scalable_queues) if scalable_queues else None
        self.form_entries: Dict[Tuple[str, str], WorkflowEntry] = {}
        self.entry_point = self._make_entry_point(self.app, self.form_entries)
        self._extend_cli(app)
        self._configure_beat_queues(app, self.beat_queue)
        self._configure_event_queue(app, self.event_queue)
        self.dynamic_parameters = DynamicParameters(
            riberry_workflow=self,
            handlers=dynamic_parameters,
            beat_queue=self.beat_queue
        )

    @classmethod
    def by_internal_name(cls, internal_name):
        return Workflow.__registered__[internal_name]

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
    def _configure_event_queue(app, event_queue):
        task_routes = {
            'riberry.celery.client.tasks.event': {'queue': event_queue},
        }

        if not app.conf.task_routes:
            app.conf.task_routes = {}
        app.conf.task_routes.update(task_routes)

    @staticmethod
    def _extend_cli(app):

        def rib_instance_cli(parser):
            parser.add_argument('--rib-instance', default=None, help='Defines the Riberry instance')

        class RiberryInstanceStep(bootsteps.StartStopStep):

            def __init__(self, worker, rib_instance=None, **options):
                super(RiberryInstanceStep, self).__init__(worker, **options)
                self.rib_instance = rib_instance or os.getenv('RIBERRY_INSTANCE')

            def start(self, worker):
                os.environ['RIBERRY_INSTANCE'] = self.rib_instance

        if not os.environ.get('RIBERRY_TESTSUITE'):
            app.user_options['worker'].add(rib_instance_cli)
            app.steps['worker'].add(RiberryInstanceStep)

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
            raise ValueError(f'Application {self.name!r} does not have an entry point with '
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

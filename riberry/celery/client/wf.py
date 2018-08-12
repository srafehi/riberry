import json
import sys
import traceback

from io import BytesIO
from typing import Union, Optional
from celery import current_task

from riberry import model, config, policy, services
from riberry.celery.client.tasks import create_event
from riberry.exc import BaseError
from riberry.model.job import ArtifactType


class TaskWrap:

    def __init__(self, func, **kwargs):
        self.func = func
        self.kwargs = kwargs

    def _mixin_kw(self, kwargs):
        return {**self.kwargs, **kwargs}

    def s(self, *args, **kwargs):
        return self.func.s(*args, **self._mixin_kw(kwargs=kwargs))

    def si(self, *args, **kwargs):
        return self.func.si(*args, **self._mixin_kw(kwargs=kwargs))

    def delay(self, *args, **kwargs):
        return self.func.delay(*args, **self._mixin_kw(kwargs=kwargs))


def step(task, step, stream=None):
    stream = stream if stream else current_task.stream
    return TaskWrap(task, __sb__=(stream, step))


def stream_start(task, stream):
    return TaskWrap(task, __ss__=stream)


def stream_end(task, stream):
    return TaskWrap(task, __se__=stream)


s = stream_start
e = stream_end
b = step


def artifact(filename: str, content: Union[bytes, str], name: str=None,
             type: Union[str, ArtifactType]=ArtifactType.output, category='Default', data: dict=None,
             stream: str=None, step=None, task_id: str=None, root_id: str=None):

    task_id = task_id or current_task.request.id
    root_id = root_id or current_task.request.root_id
    stream = stream or getattr(current_task, 'stream', None)
    step = step or getattr(current_task, 'step', None)

    if name is None:
        name = filename

    if isinstance(content, str):
        content = content.encode()

    if isinstance(type, ArtifactType):
        type = type.value

    try:
        ArtifactType(type)
    except ValueError as exc:
        raise ValueError(f'ArtifactType enum has no value {type!r}.'
                         f'Supported types: {", ".join(ArtifactType.__members__)}') from exc

    create_event(
        'artifact',
        root_id=root_id,
        task_id=task_id,
        data={
            'name': str(name),
            'type': str(type),
            'category': str(category),
            'data': data if isinstance(data, dict) else {},
            'stream': str(stream) if stream else None,
            'step': str(step) if step else None,
            'filename': str(filename),
        },
        binary=content
    )


def artifact_from_traceback(name=None, filename=None, category='Intercepted', type=model.job.ArtifactType.error):
    exc_type, exc, tb = sys.exc_info()

    if not exc_type:
        return

    if isinstance(exc, BaseError):
        error_content = f'{traceback.format_exc()}\n\n{"-"*32}\n\n{json.dumps(exc.output(), indent=2)}'.encode()
    else:
        error_content = traceback.format_exc().encode()

    artifact(
        name=name if name else f'Exception {current_task.name}',
        type=type,
        category=category,
        data={
            'Error Type': exc.__class__.__name__,
            'Error Message': str(exc)
        },
        filename=filename if filename else f'{current_task.name}-{current_task.request.id}.log',
        content=error_content
    )


def notify(notification_type, data=None, task_id=None, root_id=None):
    task_id = task_id or current_task.request.id
    root_id = root_id or current_task.request.root_id

    create_event(
        'notify',
        root_id=root_id,
        task_id=task_id,
        data={
            'type': notification_type,
            'data': data or {}
        },
        binary=None
    )


def current_execution() -> Optional[model.job.JobExecution]:
    return model.job.JobExecution.query().filter_by(
        task_id=current_task.request.root_id
    ).first()


def create_job(interface_name, interface_version, instance_name, job_name=None, input_values=None, input_files=None):
    interface: model.interface.ApplicationInterface = model.interface.ApplicationInterface.query().filter_by(
        internal_name=interface_name,
        version=interface_version
    ).first()

    instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
        internal_name=instance_name
    ).first()

    if not interface:
        raise ValueError(f'wf.create_job:: could not find ApplicationInterface with internal_name '
                         f'{interface_name!r} and version {interface_version!r}')

    if not instance:
        raise ValueError(f'wf.create_job:: could not find ApplicationInstance with internal_name '
                         f'{instance_name!r}')

    form: model.interface.Form = model.interface.Form.query().filter_by(
        instance=instance,
        interface=interface,
    ).first()

    if not form:
        raise ValueError(f'wf.create_job:: could not find Form with interface {interface} and instance {instance}')

    if not input_files:
        input_files = {}

    cleansed_input_files = {}
    for attr, value in input_files.items():
        if isinstance(value, str):
            value = value.encode()

        if isinstance(value, bytes):
            value = BytesIO(value)

        if not hasattr(value, 'read'):
            raise ValueError(f'wf.create_job:: value for input file {attr!r} must be of type str, bytes or stream')

        cleansed_input_files[attr] = value

    job_execution = current_execution()
    job_execution_user = job_execution.creator

    policy_provider = config.config.policies.provider
    with policy.context.scope(subject=job_execution_user, environment=None, policy_engine=policy_provider):
        job = services.job.create_job(
            form_id=form.id,
            name=job_name or f'Via {job_execution.job.name} / #{job_execution.id}',
            input_values=input_values or {},
            input_files=cleansed_input_files,
            execute=True,
            parent_execution=job_execution
        )

        model.conn.commit()
        return job

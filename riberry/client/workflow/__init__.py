from . import tasks
from celery import signals, current_task


import functools
from celery import Celery


def bypass(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        print(f'{args} {kwargs}')
        kwargs = {k: v for k, v in kwargs.items() if not (k.startswith('__') and k.endswith('__'))}
        return func(*args, **kwargs)
    return inner


def patch_app(app):

    def task_deco(*args, **kwargs):
        if len(args) == 1:
            if callable(args[0]):
                return Celery.task(app, bypass(*args), **kwargs)
            raise TypeError('argument 1 to @task() must be a callable')

        def inner(func):
            return Celery.task(app, **kwargs)(bypass(func))

        return inner

    app.task = task_deco
    return app


@signals.before_task_publish.connect
def before_task_publish(sender, headers, body, **_):
    if not current_task:
        return

    root_id = current_task.request.root_id
    args, kwargs, *_ = body
    task_id = headers['id']

    if '__ss__' in kwargs:
        tasks.create_event(
            name='stream',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': kwargs['__ss__'],
                'state': 'QUEUED'
            }
        )

    if '__sb__' in kwargs:
        stream, step = kwargs['__sb__']
        tasks.create_event(
            name='step',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': stream,
                'step': step,
                'state': 'QUEUED'
            }
        )


# def make_wrapper(task):
#     old_run = task.run
#
#     def new_run(*args, **kwargs):
#         kwargs = {k: v for k, v in kwargs.items() if not k.startswith('__') and not k.endswith('__')}
#         return old_run(*args, **kwargs)
#
#     task.run = new_run


@signals.task_prerun.connect
def task_prerun(sender, kwargs, **_):
    task_id = current_task.request.id
    root_id = current_task.request.root_id
    current_task.stream = None
    current_task.step = None

    if '__ss__' in kwargs:
        stream = kwargs['__ss__']
        print(f'{current_task.stream} {stream}')
        current_task.stream = stream
        tasks.create_event(
            name='stream',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': stream,
                'state': 'ACTIVE'
            }
        )

    if '__se__' in kwargs:
        stream = kwargs['__se__']
        current_task.stream = stream

    if '__sb__' in kwargs:
        stream, step = kwargs['__sb__']
        current_task.stream = stream
        current_task.step = step
        tasks.create_event(
            name='step',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': stream,
                'step': step,
                'state': 'ACTIVE'
            }
        )


@signals.task_postrun.connect
def task_postrun(sender, state, kwargs, **_):
    task_id = sender.request.id
    root_id = sender.request.root_id

    if '__ss__' in kwargs and state not in ('IGNORED', 'SUCCESS'):
        stream = kwargs['__ss__']
        tasks.create_event(
            name='stream',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': stream,
                'state': state
            }
        )

    if '__se__' in kwargs:
        stream = kwargs['__se__']
        tasks.create_event(
            name='stream',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': stream,
                'state': state
            }
        )

    if '__sb__' in kwargs:
        stream, step = kwargs['__sb__']
        current_task.stream = stream
        current_task.step = step
        tasks.create_event(
            name='step',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': stream,
                'step': step,
                'state': 'SUCCESS' if state == 'IGNORED' else state
            }
        )


class Workflow:

    def __init__(self, app, entry_point, report_task=None):
        patch_app(app)
        self.start_task = entry_point
        self.report_task = report_task
        self.input_processors = {}

    def start(self, execution_id, input_name, input_version, input_data):
        parsed_input = self.input_processors[(input_name, input_version)](execution_id=execution_id, **input_data)
        body = self.start_task.si(**parsed_input, __sb__=('primary', 'start'))
        if self.report_task:
            body |= self.report_task.s(__sb__=('primary', 'report'))

        workflow = (tasks.workflow_start.s(execution_id) | body.on_error(tasks.workflow_complete.si(status='FAILURE')) | tasks.workflow_complete.si(status='SUCCESS'))

        return workflow.delay()

    def register_input_processor(self, name, version):
        def wrapper(func):
            self.input_processors[(name, version)] = func
            return func
        return wrapper



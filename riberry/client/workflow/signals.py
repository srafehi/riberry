from celery import signals, current_task

from riberry.client.workflow import tasks


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

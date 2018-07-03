from celery import current_task

from riberry.celery.client.tasks import create_event


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


def artifact(name, type, category, filename, content, data=None, stream=None, step=None):
    task_id = current_task.request.id
    root_id = current_task.request.root_id
    stream = stream or getattr(current_task, 'stream', None)
    step = step or getattr(current_task, 'step', None)

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


def notify(notification_type, data=None):
    task_id = current_task.request.id
    root_id = current_task.request.root_id

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

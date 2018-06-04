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


def b(task, step, stream=None):
    stream = stream if stream else current_task.stream
    return TaskWrap(task, __sb__=(stream, step))


def s(task, stream):
    return TaskWrap(task, __ss__=stream)


def e(task, stream):
    return TaskWrap(task, __se__=stream)


def artifact(name, type, filename, content, stream=None, step=None):
    task_id = current_task.request.id
    root_id = current_task.request.root_id
    stream = stream or getattr(current_task, 'stream', None)
    step = step or getattr(current_task, 'step', None)

    create_event(
        'artefact',
        root_id=root_id,
        task_id=task_id,
        data={
            'name': name,
            'type': type,
            'stream': stream,
            'step': step,
            'filename': filename,
        },
        binary=content
    )

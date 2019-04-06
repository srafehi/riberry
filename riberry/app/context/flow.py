from contextlib import contextmanager

import riberry


class Flow:

    def __init__(self, context):
        self.context: riberry.app.context.Context = context
        self.scoped_stream = None
        self.scoped_category = None

    @contextmanager
    def stream_scope(self, stream=None, category=None):
        try:
            self.scoped_stream = stream
            self.scoped_category = category
            yield self
        finally:
            self.scoped_stream = None
            self.scoped_category = None

    def start(self, task, stream: str = None):
        return TaskWrap(
            task,
            __rib_stream_start=True,
            __rib_stream=self.cleanse_stream_name(stream),
        )

    def step(self, task, step: str = None, stream: str = None):
        return TaskWrap(
            task,
            __rib_step=step,
            __rib_stream=self.cleanse_stream_name(stream),
        )

    def end(self, task, stream: str = None):
        return TaskWrap(
            task,
            __rib_stream_end=True,
            __rib_stream=self.cleanse_stream_name(stream),
        )

    def cleanse_stream_name(self, stream: str):
        stream = stream or self.scoped_stream
        if not stream:
            raise ValueError('Stream name cannot be blank')
        return str(stream)


class TaskWrap:

    def __init__(self, func, **kwargs):
        self.func = func
        self.kwargs = kwargs

    @property
    def name(self):
        return self.func.name

    def _mixin_kw(self, kwargs):
        return {**self.kwargs, **kwargs}

    def s(self, *args, **kwargs):
        return self.func.s(*args, **self._mixin_kw(kwargs=kwargs))

    def si(self, *args, **kwargs):
        return self.func.si(*args, **self._mixin_kw(kwargs=kwargs))

    def delay(self, *args, **kwargs):
        return self.func.delay(*args, **self._mixin_kw(kwargs=kwargs))

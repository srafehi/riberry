import functools
import riberry


class RiberryApplication:
    __registered__ = {}

    ENTRY_POINT_TASK_NAME = 'riberry.core.app.entry_point'
    CHECK_EXTERNAL_TASK_NAME = 'riberry.core.app.check_external_task'

    def __init__(self, backend, name=None, addons=None):
        self.backend: riberry.app.backends.RiberryApplicationBackend = backend
        self.name = name
        self.context: riberry.app.context.Context = riberry.app.context.Context()
        self.executor = riberry.app.executor.TaskExecutor(riberry_app=self)
        self.entry_points = {}

        self.__registered__[self.name] = self

        # Register "entry point" task
        self.task(
            name=self.ENTRY_POINT_TASK_NAME,
        )(self.executor.entry_point_executor())

        # Register "external task checker" task
        self.task(
            name=self.CHECK_EXTERNAL_TASK_NAME,
            max_retries=None,
        )(self.executor.external_task_executor())

        self.addons = {
            'scale': riberry.app.addons.Scale(),
            'background': riberry.app.addons.BackgroundTasks(),
            'external-receiver': riberry.app.addons.ExternalTaskReceiver(),
            **(addons or {})
        }
        for addon in self.addons.values():
            addon.register(riberry_app=self)

    @classmethod
    def by_name(cls, name):
        return cls.__registered__[name]

    def entry_point(self, form, stream='Overall', step='Entry'):
        def wrapper(func):
            self.entry_points[form] = EntryPoint(
                form=form,
                func=func,
                stream=stream,
                step=step,
            )

        return wrapper

    def task(self, func=None, **options):
        if callable(func):
            wrapped_func, options = self.executor.riberry_task_executor_wrapper(func=func, task_options=options)
            return self.register_task(func=wrapped_func, **options)
        else:
            return functools.partial(self.task, **options)

    def register_task(self, func, **kwargs):
        return self.backend.register_task(func=func, **kwargs)

    def start(self, execution_id, root_id, form) -> str:
        if form not in self.entry_points:
            raise ValueError(f'Application {self.name!r} does not have an entry point with '
                             f'name {form!r} registered.')

        entry_point: EntryPoint = self.entry_points[form]
        with self.context.flow.stream_scope(stream=entry_point.stream):
            return self.backend.start_execution(execution_id=execution_id, root_id=root_id, entry_point=entry_point)


class EntryPoint:

    def __init__(self, form, func, stream, step):
        self.form = form
        self.func = func
        self.stream = stream
        self.step = step

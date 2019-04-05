import functools

import celery

import riberry


class RiberryApplication:
    __registered__ = {}

    ENTRY_POINT_TASK_NAME = 'riberry.core.app.entry_point'
    CHECK_EXTERNAL_TASK_NAME = 'riberry.core.app.check_external_task'

    def __init__(self, celery_app, name=None, addons=None):
        self.celery_app: celery.Celery = celery_app
        self.name = name
        self.context: riberry.celery.app.context.Context = riberry.celery.app.context.Context()
        self.executor = riberry.celery.app.executor.TaskExecutor(riberry_app=self)
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
            'scale': riberry.celery.app.addons.Scale(),
            'background': riberry.celery.app.addons.BackgroundTasks(),
            'external-receiver': riberry.celery.app.addons.ExternalTaskReceiver(),
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
            return self.register_task(wrapped_func, **options)
        else:
            return functools.partial(self.task, **options)

    def register_task(self, func, **kwargs):
        return self.celery_app.task(**kwargs)(func)

    def start(self, execution_id, root_id, form):
        if form not in self.entry_points:
            raise ValueError(f'Application {self.name!r} does not have an entry point with '
                             f'name {form!r} registered.')

        entry_point: EntryPoint = self.entry_points[form]
        with self.context.flow.stream_scope(stream=entry_point.stream):
            body = self.celery_app.tasks[self.ENTRY_POINT_TASK_NAME].si(
                execution_id=execution_id,
                form=form
            )

        callback_success = riberry.celery.app.tasks.execution_complete.si(status='SUCCESS', stream=entry_point.stream)
        callback_failure = riberry.celery.app.tasks.execution_complete.si(status='FAILURE', stream=entry_point.stream)

        body.options['root_id'] = root_id
        callback_success.options['root_id'] = root_id
        callback_failure.options['root_id'] = root_id

        task = body.on_error(callback_failure) | callback_success
        task.options['root_id'] = root_id

        return task.apply_async()


class EntryPoint:

    def __init__(self, form, func, stream, step):
        self.form = form
        self.func = func
        self.stream = stream
        self.step = step

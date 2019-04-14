import riberry


class RiberryApplicationConfig:

    def __init__(self, **kwargs):
        self.enable_steps = kwargs.get('enable_steps', True)


class RiberryApplication:
    __registered__ = {}

    def __init__(self, *, name, backend, config=None, addons=None):
        self.name = name
        self.config = config or RiberryApplicationConfig()
        self.__registered__[self.name] = self
        self.context: riberry.app.context.Context = riberry.app.context.Context()
        self.entry_points = {}
        self.backend: riberry.app.backends.RiberryApplicationBackend = backend
        self.backend.initialize()
        self.addons = {
            **self.backend.default_addons(),
            **(addons or {})
        }
        for addon in self.addons.values():
            if addon is not None:
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
        return self.backend.task(func=func, **options)

    def register_task(self, func, **options):
        return self.backend.register_task(func=func, **options)

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

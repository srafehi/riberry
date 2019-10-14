import os
import signal
import threading
from typing import AnyStr, Dict, Optional

import riberry
from riberry.app import RiberryApplication, current_context as cxt
from . import tasks
from .task_queue import TaskQueue, Task, TaskDefinition
from .tracker import PoolExecutionTracker

log = riberry.log.make(__name__)


def entry_point(form_name, **kwargs):
    form: riberry.model.interface.Form = riberry.model.interface.Form.query().filter_by(internal_name=form_name).one()
    try:
        app: RiberryApplication = RiberryApplication.by_name(form.application.internal_name)
    except KeyError:
        RiberryApplication(name=form.application.internal_name, backend=RiberryPoolBackend())
        app: RiberryApplication = RiberryApplication.by_name(form.application.internal_name)

    return app.entry_point(form_name, **kwargs)


class RiberryPoolBackend(riberry.app.backends.RiberryApplicationBackend):
    _local = threading.local()

    execution_tracker: PoolExecutionTracker

    def __init__(self):
        super().__init__(instance=None)
        self.task_queue: TaskQueue = TaskQueue(backend=self, limit=3)
        self.tasks = {}
        self._exit = threading.Event()
        self._threads = []
        self._pool_execution_tracker = PoolExecutionTracker(backend=self)

    def _create_thread(self, thread_name, target):
        thread = threading.Thread(name=thread_name, target=target)
        self._threads.append(thread)
        return thread

    def start(self):
        log.debug('Starting application %s', riberry.app.current_riberry_app.name)

        for sig in ('SIGTERM', 'SIGINT', 'SIGHUP'):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), self._stop_signal)

        for thread in self._threads:
            thread.start()

        log.info('Started application %s', riberry.app.current_riberry_app.name)
        for thread in self._threads:
            thread.join()

    # noinspection PyUnusedLocal
    def _stop_signal(self, signum, frame):
        self.stop()

    def stop(self):
        log.info('Stopping application %s', riberry.app.current_riberry_app.name)
        self._exit.set()

    def initialize(self):
        self._create_thread('backend.executor', lambda: tasks.run_task(
            name='Task Executor',
            func=lambda: tasks.execution_listener(self.task_queue),
            interval=0,
            exit_event=self._exit,
        ))

        self._create_thread('backend.queue_external', lambda: tasks.run_task(
            name='External Task Receiver',
            func=lambda: tasks.queue_receiver_tasks(self.task_queue),
            interval=5,
            exit_event=self._exit,
        ))

        self._create_thread('backend.background', lambda: tasks.run_task(
            name='Background Operations',
            func=lambda: tasks.background(self.task_queue),
            interval=5,
            exit_event=self._exit,
        ))

    def default_addons(self) -> Dict[AnyStr, 'riberry.app.addons.Addon']:
        return {}

    def start_execution(self, execution_id, root_id, entry_point) -> AnyStr:
        self.task_queue.submit_entry_task(
            execution_id=execution_id,
            root_id=root_id,
            entry_point=entry_point
        )
        return root_id

    def create_receiver_task(self, external_task_id, validator):
        cxt.data.set(f'external:{external_task_id}:validator', validator)

    def register_task(self, func, name=None, stream=None, step=None, **options):
        name = name or riberry.app.util.misc.function_path(func)
        assert name not in self.tasks, f'Multiple registrations for task {name!r}'
        assert 'after' in options and not self.external_task_callback(options['after']), (
            f'"after" argument not supplied to task {name!r}'
        )
        self.tasks[name] = TaskDefinition(
            func=func,
            name=name,
            stream=stream or self.default_stream_name,
            step=step or name,
            options=options,
        )

    def task_by_name(self, name: AnyStr) -> TaskDefinition:
        return self.tasks[name]

    def external_task_callback(self, name: AnyStr) -> Optional[TaskDefinition]:
        for task in self.tasks.values():
            if task.options.get('after') == name:
                return task
        return None

    def active_task(self):
        return getattr(self._local, 'task', None)

    def set_active_task(self, task: Task):
        self._local.task = task

    def _execution_tracker(self):
        return self._pool_execution_tracker

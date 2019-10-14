import threading
import uuid
from functools import wraps
from queue import Queue, Full

import riberry


class TaskDefinition:

    def __init__(self, func, name, stream, step, options):
        self.func = func
        self.name = name
        self.stream = stream
        self.step = step
        self.options = options


class Task:

    def __init__(self, task_id: str, execution_id: int, definition: TaskDefinition):
        self.id = task_id
        self.execution_id = execution_id
        self.definition = definition


class TaskCounter:

    def __init__(self):
        self._value = 0
        self._lock = threading.RLock()

    def increment(self):
        with self._lock:
            self._value += 1
            return self._value

    def decrement(self):
        with self._lock:
            self._value -= 1
            return self._value

    @property
    def lock(self):
        return self._lock

    @property
    def value(self):
        return self._value


class TaskQueue:
    queue_cls = Queue

    def __init__(self, backend, queue=None, limit=None):
        self.backend: riberry.app.backends.impl.pool.RiberryPoolBackend = backend
        self.queue = queue or self.queue_cls()
        self.limit = limit
        self.counter = TaskCounter()

    @property
    def lock(self):
        return self.counter.lock

    def limit_reached(self):
        return bool(self.limit is not None and self.counter.value >= self.limit)

    def submit_receiver_task(self, external_task: riberry.model.job.JobExecutionExternalTask):
        self.backend.execution_tracker.track_execution(
            root_id=external_task.job_execution.task_id,
            app_instance=external_task.job_execution.job.instance,
        )
        self._submit(make_receiver_task(backend=self.backend, external_task=external_task))

    def submit_entry_task(self, execution_id: int, root_id: str, entry_point: riberry.app.base.EntryPoint):
        self._submit(make_entry_task(execution_id=execution_id, root_id=root_id, entry_point=entry_point))

    def _submit(self, task: Task):
        with self.lock:
            if self.limit_reached():
                raise Full
            self.queue.put_nowait(task)
            self.counter.increment()


def _make_external_task_wrapper(task_id, func):
    @wraps(func)
    def wrapper():
        task = riberry.model.job.JobExecutionExternalTask.query().filter_by(id=task_id).one()
        task.status = 'COMPLETE'
        riberry.model.conn.commit()
        func(task.output_data)

    return wrapper


def make_receiver_task(backend, external_task: riberry.model.job.JobExecutionExternalTask) -> Task:
    definition = backend.external_task_callback(external_task.name)
    return Task(
        task_id=str(uuid.uuid4()),
        execution_id=external_task.job_execution.id,
        definition=TaskDefinition(
            func=_make_external_task_wrapper(external_task.id, definition.func),
            name=definition.step,
            stream=definition.stream,
            step=definition.step,
            options=definition.options,
        ),
    )


def make_entry_task(execution_id: int, root_id: str, entry_point: riberry.app.base.EntryPoint):
    return Task(
        task_id=root_id,
        execution_id=execution_id,
        definition=TaskDefinition(
            func=entry_point.func,
            name=entry_point.step,
            stream=entry_point.stream,
            step=entry_point.step,
            options={},
        )
    )

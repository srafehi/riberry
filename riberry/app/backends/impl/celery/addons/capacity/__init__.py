from sqlalchemy.util.compat import contextmanager

from ..base import AddonStartStopStep
from .priority_queue import PriorityQueue
import riberry
from celery.utils.log import logger as log


class Capacity(riberry.app.addons.Addon):

    def __init__(self, parameter='capacity', key=None, sep='|', queue_cls=PriorityQueue, blocking: bool = True, block_retry: int = 0.5, r=None):
        self.parameter = parameter
        self.r = r or riberry.celery.util.celery_redis_instance()
        self.sep = sep
        self.queue = queue_cls(r=self.r, key=key, blocking=blocking, block_retry=block_retry)

    @property
    def last_value_key(self):
        return self.queue.make_key(self.queue.key, 'raw')

    @property
    def last_value(self):
        last_value = self.r.get(self.last_value_key)
        return last_value.decode() if isinstance(last_value, bytes) else None

    @last_value.setter
    def last_value(self, value):
        self.r.set(self.last_value_key, value=value)

    @contextmanager
    def borrow(self):
        member, score, version = self.queue.pop()
        try:
            yield member, score, version
        finally:
            self.queue.put(member=member, version=version)

    def register(self, riberry_app: 'riberry.app.base.RiberryApplication'):
        class ConcreteCapacityStep(CapacityStep):
            rib = riberry_app
            capacity = self

        if self.queue.key is None:
            self.queue.key = riberry_app.context.current.riberry_app_instance.internal_name

        riberry_app.backend.steps['worker'].add(ConcreteCapacityStep)


class CapacityStep(AddonStartStopStep):
    capacity: Capacity

    def __init__(self, worker, **_):
        super().__init__(worker=worker, interval=1.0)
        self.lock = riberry.app.util.redis_lock.RedisLock(name='step:capacity', on_acquired=self.on_lock_acquired, interval=900)

    def should_run(self) -> bool:
        return True

    def on_lock_acquired(self):
        value = self.rib.context.current.riberry_app_instance.active_schedule_value(name=self.capacity.parameter) or ''

        if self.capacity.last_value is not None and value == self.capacity.last_value:
            log.warn(f'DynamicPriorityParameter: ({self.capacity.queue.free_key}) is unchanged')
            return

        self.capacity.last_value = value
        values = [
            part.split(self.capacity.sep) if self.capacity.sep in part else (part, 0)
            for part in value.split(' ')
        ]

        member_scores = {k: int(v) for k, v in values}
        self.capacity.queue.update(member_scores)
        log.warn(f'DynamicPriorityParameter: ({self.capacity.queue.free_key}) updated {self.capacity.parameter} queue with {value!r}')

    def run(self):
        redis_instance = riberry.celery.util.celery_redis_instance()
        self.lock.run(redis_instance=redis_instance)

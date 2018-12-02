from contextlib import contextmanager

from celery import current_app
from celery.utils.log import logger

from riberry.celery import client
from riberry.celery.client.dynamic import DynamicParameter
from riberry.celery.client.dynamic.util import PriorityQueue
from riberry.celery.client.scale import redis_queues_empty_workers_idle, ConcurrencyScale
from riberry.celery.util import celery_redis_instance


class DynamicQueues(DynamicParameter):

    def __init__(self, parameter='active'):
        super(DynamicQueues, self).__init__(parameter=parameter)

    def on_received(self, instance, value):
        scale = ConcurrencyScale.instance()
        value = str(value).upper()

        if value == 'N':
            logger.info('DynamicQueues: app down')
            current_app.control.broadcast('scale_down', arguments={'instance': client.current_instance_name()})
        elif scale and redis_queues_empty_workers_idle(scale.target_queues):
            logger.info('DynamicQueues: empty queues')
            current_app.control.broadcast('scale_down', arguments={'instance': client.current_instance_name()})
        elif value == 'Y':
            logger.info('DynamicQueues: scaling up')
            current_app.control.broadcast('scale_up', arguments={'instance': client.current_instance_name()})


class DynamicConcurrency(DynamicParameter):

    def __init__(self, parameter='concurrency'):
        super(DynamicConcurrency, self).__init__(parameter=parameter)

    def on_received(self, instance, value):
        current_app.control.broadcast('scale_to', arguments={
            'concurrency': int(value),
            'instance': client.current_instance_name()
        })


class DynamicPriorityParameter(client.dynamic.DynamicParameter):

    def __init__(
            self, parameter='hosts', key=None, sep='|', queue_cls=PriorityQueue,
            blocking: bool=True, block_retry: int=0.5, r=None):
        super(DynamicPriorityParameter, self).__init__(parameter=parameter)
        self.r = r or celery_redis_instance()
        self.sep = sep
        self.queue = queue_cls(
            r=self.r,
            key=key or client.current_instance_name(raise_on_none=True),
            blocking=blocking,
            block_retry=block_retry,
        )

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

    def on_received(self, instance, value):
        if self.last_value is not None and value == self.last_value:
            return

        self.last_value = value
        values = [
            part.split(self.sep) if self.sep in part else (part, 0)
            for part in (value or '').split(' ')
        ]

        member_scores = {k: int(v) for k, v in values}
        self.queue.update(member_scores)
        logger.info(f'DynamicPriorityParameter: ({self.queue.free_key}) updated {self.parameter} queue with {value!r}')

    @contextmanager
    def borrow(self):
        member, score, version = self.queue.pop()
        try:
            yield member, score, version
        finally:
            self.queue.put(member=member, version=version)

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

    def __init__(self, parameter='hosts', key=None, sep='|', queue_cls=PriorityQueue, r=None):
        super(DynamicPriorityParameter, self).__init__(parameter=parameter)
        self.r = r or celery_redis_instance()
        self.sep = sep
        self.queue = queue_cls(r=self.r, key=key or client.current_instance_name(raise_on_none=True))
        self.queue.clear()
        self.last_value = None

    def on_received(self, instance, value):
        if value == self.last_value:
            return

        values = [
            value.split(self.sep) if self.sep in value else (value, 0)
            for value in value.split(' ')
        ]

        member_scores = {k: int(v) for k, v in values}
        self.queue.update(member_scores)

    @contextmanager
    def borrow(self):
        member, score, version = self.queue.pop()
        try:
            yield member, score, version
        finally:
            self.queue.put(member=member, version=version)

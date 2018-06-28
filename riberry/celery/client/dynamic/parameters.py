from celery import current_app
from celery.utils.log import logger

from riberry.celery.client.scale import redis_queues_empty_workers_idle, ConcurrencyScale
from riberry.celery.client.dynamic import DynamicParameter


class DynamicWorkers(DynamicParameter):

    def __init__(self, scale: ConcurrencyScale, parameter='active'):
        super(DynamicWorkers, self).__init__(parameter=parameter)
        self.scale = scale

    def on_received(self, instance, value):
        value = str(value).upper()
        if value == 'N':
            logger.info('DynamicWorkers: app down')
            current_app.control.broadcast('scale_down')
        elif redis_queues_empty_workers_idle(self.scale.target_queues):
            logger.info('DynamicWorkers: empty queues')
            current_app.control.broadcast('scale_down')
        elif value == 'Y':
            logger.info('DynamicWorkers: scaling up')
            current_app.control.broadcast('scale_up')


class DynamicConcurrency(DynamicParameter):

    def __init__(self, parameter='concurrency'):
        super(DynamicConcurrency, self).__init__(parameter=parameter)

    def on_received(self, instance, value):
        current_app.control.broadcast('scale_to', arguments={
            'concurrency': int(value)
        })

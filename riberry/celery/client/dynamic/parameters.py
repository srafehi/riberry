from celery import current_app
from celery.utils.log import logger

from riberry.celery import client
from riberry.celery.client.dynamic import DynamicParameter
from riberry.celery.client.scale import redis_queues_empty_workers_idle, ConcurrencyScale


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

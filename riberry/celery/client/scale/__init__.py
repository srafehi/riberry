from urllib.parse import urlparse

import redis
from celery import current_app, signals
from celery.utils.log import logger
from celery.worker import control

from riberry.celery import client


class ConcurrencyScale:
    all_apps = {}

    def __init__(self, app, target_queues=None):
        self.all_apps[app.main] = self
        self.target_queues = set(target_queues or [])
        self.initial_queues = set()
        self.current_concurrency = None

    @property
    def target_worker_queues(self):
        return self.initial_queues & self.target_queues

    def scale_down(self, consumer):
        current_concurrency = consumer.pool.num_processes

        queues_to_skip = set()
        for queue in consumer.task_consumer.queues:
            self.initial_queues.add(queue.name)
            if queue.name not in self.target_queues:
                queues_to_skip.add(queue.name)
            else:
                logger.info(f'scale-down - removing queue: {queue.name}')
                consumer.cancel_task_queue(queue)

        if self.current_concurrency is None:
            self.current_concurrency = current_concurrency

        if not queues_to_skip and current_concurrency > 0:
            self.current_concurrency = current_concurrency
            logger.info(f'scale-down - shrink amount: {current_concurrency}')
            consumer.pool.shrink(current_concurrency)

    def scale_up(self, consumer):
        current_concurrency = consumer.pool.num_processes

        if self.current_concurrency is None:
            return

        queue_names = {q.name for q in consumer.task_consumer.queues}
        for queue in self.initial_queues:
            if queue not in queue_names:
                logger.info(f'scale-up - adding queue: {queue}')
                consumer.add_task_queue(queue)

        processes_to_add = self.current_concurrency - current_concurrency
        max_processes_to_add = min(processes_to_add, 8)
        if processes_to_add > 0:
            logger.info(f'scale-up - grow amount: {max_processes_to_add}')
            consumer.pool.grow(processes_to_add)

    def scale_to(self, consumer, concurrency):

        queue_names = {q.name for q in consumer.task_consumer.queues}
        if not set(queue_names) & self.target_queues:
            logger.debug(
                f'scale-to: no target queues '
                f'current={", ".join(queue_names or ["N/A"])} target={", ".join(self.target_queues or ["N/A"])}')
            return

        current_concurrency = consumer.pool.num_processes
        process_diff = concurrency - current_concurrency
        process_diff = min(process_diff, 8)
        if process_diff > 0:
            logger.info(f'scale-to: +{process_diff} -> new: {current_concurrency + process_diff} target: {concurrency}')
            consumer.pool.grow(process_diff)
        elif process_diff < 0 and concurrency:
            logger.info(f'scale-to: {process_diff} -> new: {current_concurrency + process_diff} target: {concurrency}')

            consumer.qos.decrement_eventually(consumer.qos.value - 1)
            consumer.qos.update()

            try:
                consumer.pool.shrink(abs(process_diff))
            except Exception as exc:
                logger.error(exc)
                return
        else:
            prefetch_count = (consumer.pool.num_processes * consumer.prefetch_multiplier) - consumer.qos.value
            consumer.qos.increment_eventually(n=prefetch_count)

        self.current_concurrency = process_diff + current_concurrency

    @classmethod
    def instance(cls) -> 'ConcurrencyScale':
        return cls.all_apps.get(current_app.main)


@signals.worker_ready.connect
def scale_down_on_startup(sender, **_):
    scale = ConcurrencyScale.instance()
    scale and scale.scale_down(consumer=sender)


@control.control_command()
def scale_down(state, instance):
    scale = ConcurrencyScale.instance()
    scale and instance and client.is_current_instance(instance_name=instance) and scale.scale_down(
        consumer=state.consumer)


@control.control_command()
def scale_up(state, instance):
    scale = ConcurrencyScale.instance()
    scale and instance and client.is_current_instance(instance_name=instance) and scale.scale_up(
        consumer=state.consumer)


@control.control_command()
def scale_to(state, concurrency, instance):
    scale = ConcurrencyScale.instance()
    scale and instance and client.is_current_instance(instance_name=instance) and scale.scale_to(
        consumer=state.consumer,
        concurrency=concurrency)


@control.control_command()
def worker_task_count(state, instance):
    if not instance or not client.is_current_instance(instance_name=instance):
        return 0

    scale = ConcurrencyScale.instance()
    if not scale or not scale.target_worker_queues:
        return 0

    return sum([len(control.scheduled(state)), len(control.active(state)), len(control.reserved(state))])


def redis_queues_empty_workers_idle(queues):
    broker_uri = current_app.connection().as_uri(include_password=True)
    url = urlparse(broker_uri)
    r = redis.Redis(host=url.hostname, port=url.port, password=url.password)

    separator = '\x06\x16'
    priority_steps = [0, 3, 6, 9]
    for queue in queues:
        for prio in priority_steps:
            queue = f'{queue}{separator}{prio}' if prio else queue
            queue_length = r.llen(queue)
            if queue_length:
                return False

    all_worker_counts = current_app.control.broadcast(
        'worker_task_count',
        reply=True,
        arguments={
            'instance': client.current_instance_name()
        }
    )

    if any([sum(r.values()) for r in all_worker_counts]):
        return False

    return True

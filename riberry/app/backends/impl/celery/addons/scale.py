import math

import riberry
from .base import AddonStartStopStep
from celery.utils.log import logger as log


class Scale(riberry.app.addons.Addon):

    def __init__(
            self,
            active_parameter='active',
            concurrency_parameter='concurrency',
            minimum_concurrency=None,
            maximum_concurrency=None,
            check_queues=None,
    ):
        self.conf = ScaleConfiguration(
            active_parameter=active_parameter,
            concurrency_parameter=concurrency_parameter,
            minimum_concurrency=minimum_concurrency,
            maximum_concurrency=maximum_concurrency,
            check_queues=check_queues,
        )
        self.active_parameter = active_parameter
        self.concurrency_parameter = concurrency_parameter
        self.minimum_concurrency = minimum_concurrency
        self.maximum_concurrency = maximum_concurrency

    def register(self, riberry_app):
        class ConcreteScaleStep(ScaleStep):
            conf = self.conf
            rib = riberry_app

        riberry_app.backend.steps['worker'].add(ConcreteScaleStep)
        riberry_app.backend.user_options['worker'].add(self.regiser_user_options)

    @staticmethod
    def regiser_user_options(parser):
        parser.add_argument(
            '--rib-scale', action='store_true', default=False,
            help='Scale concurrency depending on activity (disabled by default)',
        )
        parser.add_argument(
            '--rib-scale-parameter',
            help='The name of the application instance parameter which stores the concurrency value',
        )
        parser.add_argument(
            '--rib-scale-group', default='default',
            help='Concurrency is distributed amongst all active workers in the same group',
        )
        parser.add_argument(
            '--rib-scale-max', default=None,
            help='Maximum concurrency when auto-scaling (uncapped by default)',
        )
        parser.add_argument(
            '--rib-scale-min', default=None,
            help='Minimum concurrency when auto-scaling (zero by default)',
        )

        feature_parser = parser.add_mutually_exclusive_group(required=False)
        feature_parser.add_argument(
            '--rib-scale-check-queues', dest='rib_scale_check_queues', action='store_true',
            help='Scale down if queues are empty and there are no active tasks'
        )
        feature_parser.add_argument(
            '--rib-scale-ignore-queues', dest='rib_scale_check_queues', action='store_false',
            help='Do not scale down if queues are empty and there are no active tasks'
        )
        parser.set_defaults(rib_scale_check_queues=True)


class ScaleConfiguration:

    def __init__(
            self,
            scale_concurrency=False,
            scale_group='default',
            active_parameter='active',
            concurrency_parameter='concurrency',
            minimum_concurrency=None,
            maximum_concurrency=None,
            check_queues=None,
    ):
        self.active_parameter = active_parameter
        self.concurrency_parameter = concurrency_parameter
        self.scale = scale_concurrency
        self.scale_group = scale_group
        self.minimum_concurrency = minimum_concurrency
        self.maximum_concurrency = maximum_concurrency
        self.check_queues = check_queues
        self.ignore_queues = set()


class ScaleStep(AddonStartStopStep):

    conf: ScaleConfiguration

    def __init__(self, worker, rib_scale, rib_scale_group, rib_scale_parameter, rib_scale_min, rib_scale_max, rib_scale_check_queues, **_):
        super().__init__(worker=worker, interval=1.0)

        self.conf.scale = bool(rib_scale)
        self.conf.scale_group = rib_scale_group or self.conf.scale_group
        self.conf.concurrency_parameter = rib_scale_parameter or self.conf.concurrency_parameter
        self.conf.minimum_concurrency = int(rib_scale_min) if rib_scale_min is not None else self.conf.minimum_concurrency
        self.conf.maximum_concurrency = int(rib_scale_max) if rib_scale_max is not None else self.conf.maximum_concurrency
        self.conf.check_queues = bool(rib_scale_check_queues) if rib_scale_check_queues is not None else self.conf.check_queues

        self.lock = riberry.app.util.redis_lock.RedisLock(name=f'step:scale:{self.conf.scale_group}', on_acquired=self.on_lock_acquired, interval=5000)

        self.queues = set()
        self.target_concurrency = None
        self.initial_concurrency = None
        self.is_active = False
        self._worker_uuid = self.rib.context.current.WORKER_UUID
        self._instance_name = self.rib.context.current.riberry_app_instance.internal_name
        self.idle_counter = 0

    def should_run(self) -> bool:
        return True

    def run(self):
        redis_instance = riberry.celery.util.celery_redis_instance()
        self.lock.run(redis_instance=redis_instance)
        if not self.consumer.task_consumer:
            return
        
        self.update(redis_instance=redis_instance)
        self.scale()

    def on_lock_acquired(self):
        r = riberry.celery.util.celery_redis_instance()
        epoch, _ = r.time()
        occurrence = set(r.zrevrangebyscore(name=self.scale_groups_log_key, max=epoch, min=epoch - 60))
        if occurrence:
            r.delete(self.scale_groups_active_temp_key)
            r.sadd(self.scale_groups_active_temp_key, *occurrence)
            r.rename(src=self.scale_groups_active_temp_key, dst=self.scale_groups_active_key)

    @classmethod
    def _tasks_available(cls, r, state, queues):
        return bool(
            len(state.reserved_requests) or
            len(state.active_requests) or
            state.requests or
            not cls._queues_empty(r, queues=queues)
        )

    @staticmethod
    def _queues_empty(r, queues):
        for queue_name in queues:
            for queue in r.scan_iter(f'{queue_name}*'):
                try:
                    queue_length = r.llen(queue)
                    log.debug(f'ScaleStep:: Queue length of {queue.decode()!r} is {queue_length}')
                    if queue_length:
                        return False
                except:
                    continue
        return True

    def report(self, r):
        epoch, _ = r.time()
        r.zadd(name=self.scale_groups_log_key, mapping={self._worker_uuid: epoch})

    @property
    def scale_groups_active_key(self):
        return f'{self._instance_name}:scale-groups:{self.conf.scale_group}:active'

    @property
    def scale_groups_log_key(self):
        return f'{self._instance_name}:scale-groups:{self.conf.scale_group}:log'

    @property
    def scale_groups_active_temp_key(self):
        return f'{self._instance_name}:scale-groups:{self.conf.scale_group}:active-temp'

    def update(self, redis_instance):
        self.queues.update({q.name for q in self.worker.consumer.task_consumer.queues})
        if not self.initial_concurrency:
            self.initial_concurrency = self.worker.consumer.pool.num_processes

        instance = self.rib.context.current.riberry_app_instance
        active_flag = instance.active_schedule_value(name=self.conf.active_parameter, default='Y') == 'Y'
        tasks_available = self._tasks_available(r=redis_instance, state=self.worker_state, queues=self.queues - self.conf.ignore_queues)
        if tasks_available:
            self.report(r=redis_instance)

        self.is_active = active_flag and (tasks_available if self.conf.check_queues else True)
        if not self.is_active:
            if self.idle_counter > 10 or self.target_concurrency is None:
                self.target_concurrency = 0
            self.idle_counter += 1
            return
        self.idle_counter = 0

        scale_group = list(sorted(b.decode() for b in redis_instance.smembers(self.scale_groups_active_key)))
        if self.conf.scale and self.conf.concurrency_parameter:
            target_concurrency = instance.active_schedule_value(name=self.conf.concurrency_parameter, default=None)
            if target_concurrency is None:
                target_concurrency = self.initial_concurrency
            else:
                target_concurrency = int(target_concurrency)
            target_concurrency *= (1 / len(scale_group)) if self._worker_uuid in scale_group else 0

            if target_concurrency:
                if scale_group.index(self._worker_uuid) == 0:
                    target_concurrency = math.ceil(target_concurrency)
                else:
                    target_concurrency = math.floor(target_concurrency)
        else:
            target_concurrency = self.initial_concurrency

        if self.conf.maximum_concurrency is not None:
            target_concurrency = min(target_concurrency, self.conf.maximum_concurrency)

        if self.conf.minimum_concurrency is not None:
            target_concurrency = max(target_concurrency, self.conf.minimum_concurrency)

        target_concurrency = int(target_concurrency)
        if target_concurrency != self.target_concurrency:
            self.target_concurrency = target_concurrency

    def scale(self):
        actual_concurrency = self.worker.consumer.pool.num_processes
        target_concurrency = self.target_concurrency

        scale_group = list(sorted(b.decode() for b in riberry.celery.util.celery_redis_instance().smembers(self.scale_groups_active_key)))
        log.debug(f'A: {self.is_active} C[T]: {self.target_concurrency}, C[A]: {actual_concurrency}, P: {self.worker.consumer.qos.value},  M: {scale_group}')

        if target_concurrency == 0:
            for queue in list(self.consumer.task_consumer.queues):
                if queue.name not in self.conf.ignore_queues:
                    self.consumer.cancel_task_queue(queue)
        else:
            queue_names = {q.name for q in self.consumer.task_consumer.queues}
            for queue in self.queues:
                if queue not in queue_names and queue not in self.conf.ignore_queues:
                    self.consumer.add_task_queue(queue)

        if target_concurrency > actual_concurrency:
            if actual_concurrency == 0:
                self.worker.consumer.pool.grow(1)
            else:
                self.worker.consumer.pool.grow(min(target_concurrency - actual_concurrency, 8))
            log.info(f'ScaleStep:: Scaled concurrency up to {self.worker.consumer.pool.num_processes} concurrency (target: {self.target_concurrency}, prefetch: {self.worker.consumer.qos.value})')
        elif actual_concurrency > target_concurrency:
            self.worker.consumer.qos.decrement_eventually(n=1)
            self.worker.consumer.qos.update()
            self.worker.consumer.pool.shrink(min(actual_concurrency - target_concurrency, 8))
            log.info(f'ScaleStep:: Scaled concurrency down to {self.worker.consumer.pool.num_processes} concurrency (target: {self.target_concurrency}, prefetch: {self.worker.consumer.qos.value})')

        prefetch_target = self.worker.consumer.pool.num_processes * self.worker.consumer.prefetch_multiplier
        prefetch_difference = prefetch_target - self.worker.consumer.qos.value
        if prefetch_difference > 0:
            self.worker.consumer.qos.increment_eventually(n=abs(prefetch_difference))
            self.worker.consumer.qos.update()
            log.info(f'ScaleStep:: Scaled prefetch up to {self.worker.consumer.qos.value} (+{prefetch_difference})')
        elif prefetch_difference < 0 and self.worker.consumer.qos.value != 1:
            self.worker.consumer.qos.decrement_eventually(n=abs(prefetch_difference))
            self.worker.consumer.qos.update()
            log.info(f'ScaleStep:: Scaled prefetch down to {self.worker.consumer.qos.value} ({prefetch_difference})')


import os

from celery import current_app, bootsteps, signals
from celery.utils.log import logger
from celery.worker import control

from . import predicates


def make_check_status(app, predicates):
    def inner():
        for task in predicates:
            if task():
                app.control.broadcast('scale_down')
                break
        else:
            app.control.broadcast('scale_up')

    inner.__name__ = 'monitor'
    return inner


def add_worker_arguments(parser):
    parser.add_argument(
        '--scale', action='store_true', default=False,
        help='Scale the workers based on some events.',
    )


class ScaleBootstep(bootsteps.Step):

    def __init__(self, worker, scale=False, **_):
        super(ScaleBootstep, self).__init__(worker)
        if scale:
            os.environ['CELERY_SCALE_WORKER'] = '1'


class CeleryScale:
    all_apps = {}

    def __init__(self, app, predicates, beat_queue):
        self.app = app
        self.initial_queues = set()
        self.initial_concurrency = None
        self.all_apps[app.main] = self
        app.task(name='scale-workers')(make_check_status(app=app, predicates=predicates))
        self._configure_beat_queues(app, beat_queue)

        app.user_options['worker'].add(add_worker_arguments)
        app.steps['worker'].add(ScaleBootstep)


    @staticmethod
    def _configure_beat_queues(app, beat_queue):
        schedule = {
            'scale-workers': {
                'task': 'scale-workers',
                'schedule': 5,
                'options': {'queue': beat_queue}
            },
        }

        if not app.conf.beat_schedule:
            app.conf.beat_schedule = {}
        app.conf.beat_schedule.update(schedule)

    @property
    def should_scale(self):
        return os.getenv('CELERY_SCALE_WORKER') == '1'

    def scale_down_workers(self, consumer):
        for queue in consumer.task_consumer.queues:
            self.initial_queues.add(queue.name)
            consumer.cancel_task_queue(queue)

        if self.initial_concurrency is None:
            self.initial_concurrency = consumer.pool.num_processes

        if consumer.pool.num_processes > 0:
            logger.info('Scaling down worker')
            consumer.pool.shrink(consumer.pool.num_processes)

    def scale_up_workers(self, consumer):
        queue_names = {q.name for q in consumer.task_consumer.queues}
        for queue in self.initial_queues:
            if queue not in queue_names:
                consumer.add_task_queue(queue)

        processes_to_add = self.initial_concurrency - consumer.pool.num_processes
        if processes_to_add > 0:
            logger.info('Scaling up worker')
            consumer.pool.grow(processes_to_add)

    @classmethod
    def instance(cls) -> 'CeleryScale':
        return cls.all_apps.get(current_app.main)


@signals.worker_ready.connect
def scale_down_on_startup(sender, **_):
    scaler = CeleryScale.instance()
    if scaler and scaler.should_scale:
        scaler.scale_down_workers(consumer=sender)


@control.control_command()
def scale_down(state):
    scaler = CeleryScale.instance()
    if scaler and scaler.should_scale:
        scaler.scale_down_workers(consumer=state.consumer)


@control.control_command()
def scale_up(state):
    scaler = CeleryScale.instance()
    if scaler and scaler.should_scale:
        scaler.scale_up_workers(consumer=state.consumer)


@control.control_command()
def worker_task_count(state):
    scaler = CeleryScale.instance()
    if scaler and scaler.should_scale:
        return sum([len(control.scheduled(state)), len(control.active(state)), len(control.reserved(state))])
    return 0

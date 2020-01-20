from celery import Celery
from riberry import config

app = Celery(main='background-tasks')

app.conf.update(config.config.celery)

app.conf.beat_schedule.update({
    'process:execution': {
        'task': 'riberry.celery.background.tasks.process_events',
        'schedule': config.config.background.events.interval,
        'kwargs': {
            'event_limit': config.config.background.events.processing_limit
        },
        'options': {'queue': 'riberry.background.events'}
    },
    'process:metrics': {
        'task': 'riberry.celery.background.tasks.update_execution_metrics',
        'schedule': config.config.background.metrics.interval,
        'kwargs': {
            'time_interval': config.config.background.metrics.time_interval,
            'step_limit': config.config.background.metrics.step_limit,
        },
        'options': {'queue': 'riberry.background.metrics'}
    },
    'process:job-schedule': {
        'task': 'riberry.celery.background.tasks.job_schedules',
        'schedule': config.config.background.schedules.interval,
        'options': {'queue': 'riberry.background.schedules'}
    },
    'process:capacity': {
        'task': 'riberry.celery.background.tasks.update_capacity_parameters',
        'schedule': config.config.background.capacity.interval,
        'options': {'queue': 'riberry.background.schedules'}
    },
})

app.conf.imports = list(app.conf.imports) + ['riberry.celery.background.tasks']


def register_task(task_path, schedule):
    app.conf.beat_schedule[task_path] = {
        'task': 'riberry.celery.background.tasks.custom_task',
        'schedule': schedule,
        'args': [task_path],
        'options': {'queue': 'riberry.background.custom'}
    }

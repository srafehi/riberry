from celery import Celery
from riberry import config

app = Celery(main='background-tasks')

app.conf.update(config.config.celery)

app.conf.beat_schedule.update({
    'process:execution': {
        'task': 'riberry.celery.background.tasks.workflow_events',
        'schedule': 2,
        'options': {'queue': 'riberry.background.executions'}
    },
    'process:job-schedule': {
        'task': 'riberry.celery.background.tasks.job_schedules',
        'schedule': 5,
        'options': {'queue': 'riberry.background.misc'}
    },
    'process:notifications': {
        'task': 'riberry.celery.background.tasks.notifications',
        'schedule': 5,
        'options': {'queue': 'riberry.background.misc'}
    }
})

app.conf.imports = list(app.conf.imports) + ['riberry.celery.background.tasks']


def register_task(task_path, schedule):
    app.conf.beat_schedule[task_path] = {
        'task': 'riberry.celery.background.tasks.custom_task',
        'schedule': schedule,
        'args': [task_path],
        'options': {'queue': 'riberry.background.custom'}
    }

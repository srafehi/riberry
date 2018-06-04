from celery import Celery

app = Celery(main='background-tasks')
app.conf.result_backend = app.conf.broker_url = 'redis://'
app.conf.beat_schedule = {
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
}


app.conf.timezone = 'UTC'
app.conf.imports = ['riberry.celery.background.tasks']


def register_task(task_path, schedule):
    app.conf.beat_schedule[task_path] = {
        'task': 'riberry.celery.background.tasks.custom_task',
        'schedule': schedule,
        'args': [task_path],
        'options': {'queue': 'riberry.background.custom'}
    }

from celery import Celery

from riberry import model
from . import events

app = Celery()
app.conf.result_backend = app.conf.broker_url = 'redis://'
app.conf.beat_schedule = {
    'process-events': {
        'task': 'riberry.client.workflow.beat.process_events',
        'schedule': 4,
        'options': {'queue': 'wf.beat.events'}
    },
    'process-schedules': {
        'task': 'riberry.client.workflow.beat.process_schedules',
        'schedule': 5,
        'options': {'queue': 'wf.beat.schedules'}
    }
}

app.conf.timezone = 'UTC'


@app.task
def process_events():
    events.process()


@app.task
def process_schedules():
    for schedule in model.job.JobSchedule.query().filter_by(enabled=True).all():
        schedule.run()

    model.conn.commit()

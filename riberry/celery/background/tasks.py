import importlib

from riberry import model
from riberry.celery.background.events import events
from . import app


@app.task(ignore_result=True)
def process_events(event_limit=None):
    events.process(event_limit)


@app.task(ignore_result=True)
def job_schedules():
    with model.conn:
        for schedule in model.job.JobSchedule.query().filter_by(enabled=True).all():
            print(schedule)
            schedule.run()

        model.conn.commit()


@app.task(ignore_result=True)
def custom_task(func_path):
    module_path, func_name = func_path.split(':')
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()

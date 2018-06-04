import importlib

from riberry import model
from riberry.celery.background.events import execution_events
from . import app


@app.task
def execution_events():
    execution_events.process()


@app.task
def job_schedules():
    for schedule in model.job.JobSchedule.query().filter_by(enabled=True).all():
        schedule.run()

    model.conn.commit()


@app.task
def notifications():
    pass


@app.task
def custom_task(func_path):
    module_path, func_name = func_path.split(':')
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()

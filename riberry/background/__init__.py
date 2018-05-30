import importlib
from celery import Celery


app = Celery(main='background-tasks')
app.conf.result_backend = app.conf.broker_url = 'redis://'
app.conf.beat_schedule = {}
app.conf.task_default_queue = 'riberry.background'

app.conf.timezone = 'UTC'
app.conf.imports = ['applications']


def register_task(task_path, schedule):
    app.conf.beat_schedule[task_path] = {
        'task': 'riberry.background.run_task',
        'schedule': schedule,
        'args': [task_path]
    }


@app.task
def run_task(func_path):
    module_path, func_name = func_path.split(':')
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()

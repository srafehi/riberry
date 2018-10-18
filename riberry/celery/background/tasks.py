import importlib

from riberry import model
from riberry.celery.background import capacity_config
from riberry.celery.background.events import events
from . import app


@app.task(ignore_result=True)
def process_events(event_limit=None):
    events.process(event_limit)


@app.task(ignore_result=True)
def job_schedules():
    with model.conn:
        for schedule in model.job.JobSchedule.query().filter_by(enabled=True).all():
            schedule.run()

        model.conn.commit()


@app.task(ignore_result=True)
def update_capacity_parameters():
    with model.conn:
        for capacity_configuration in model.application.CapacityConfiguration.query().all():
            producers = [
                capacity_config.CapacityProducer(producer.internal_name, producer.capacity) for producer in
                capacity_configuration.producers
            ]

            capacity_config.update_instance_capacities(
                producers=producers,
                weight_parameter=capacity_configuration.weight_parameter,
                capacity_parameter=capacity_configuration.capacity_parameter,
                producer_parameter=capacity_configuration.producer_parameter,
                distribution_strategy=capacity_configuration.distribution_strategy,
            )

        model.conn.commit()


@app.task(ignore_result=True)
def custom_task(func_path):
    module_path, func_name = func_path.split(':')
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()

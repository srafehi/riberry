from typing import List

from celery import current_app
from celery.utils.log import logger

from riberry import model
from riberry.celery import client


class DynamicParameter:

    def __init__(self, parameter):
        self.parameter = parameter

    def on_received(self, instance: model.application.ApplicationInstance, value: str):
        raise NotImplementedError


def make_dynamic_parameters_task(handlers: List[DynamicParameter]):

    def dynamic_parameters_task():
        if not handlers:
            return

        with model.conn:
            instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
                internal_name=client.current_instance_name(raise_on_none=True)
            ).one()
            active_schedules = instance.active_schedules

        handler_mapping = {h.parameter: h for h in handlers}
        for parameter, schedule in sorted(active_schedules.items(), key=lambda item: -item[1].priority if item[1] else 0):
            if parameter in handler_mapping:
                value = schedule.value if schedule else None
                logger.info(f'dynamic-parameters: updating dynamic parameter {parameter!r} with value {value!r}')
                try:
                    handler_mapping[parameter].on_received(instance=instance, value=value)
                except:
                    logger.exception(f'dynamic-parameters: {parameter!r} failed')

    dynamic_parameters_task.__name__ = 'dynamic'
    return dynamic_parameters_task


class DynamicParameters:
    all_apps = {}

    def __init__(self, riberry_workflow, handlers, beat_queue):
        self.workflow = riberry_workflow
        self.all_apps[self.workflow.app.main] = self
        self.workflow.app.task(name='dynamic-parameters', rib_task=False)(make_dynamic_parameters_task(handlers=handlers))
        self._configure_beat_queues(self.workflow.app, beat_queue)

    @staticmethod
    def _configure_beat_queues(app, beat_queue):
        schedule = {
            'dynamic-parameters': {
                'task': 'dynamic-parameters',
                'schedule': 2,
                'options': {'queue': beat_queue}
            },
        }

        if not app.conf.beat_schedule:
            app.conf.beat_schedule = {}
        app.conf.beat_schedule.update(schedule)

    @classmethod
    def instance(cls) -> 'DynamicParameters':
        return cls.all_apps.get(current_app.main)

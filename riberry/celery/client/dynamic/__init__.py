import os
from typing import List
from celery import current_app
from riberry import model


class DynamicParameter:

    def __init__(self, parameter):
        self.parameter = parameter
        self.last_value = None

    def on_received(self, instance: model.application.ApplicationInstance, value: str):
        raise NotImplementedError


def make_dynamic_parameters_task(handlers: List[DynamicParameter]):

    def dynamic_parameters_task():
        if not handlers:
            return

        with model.conn:
            instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
                internal_name=os.environ['RIBERRY_INSTANCE']
            ).one()
            instance_parameters = instance.parameters

        handler_mapping = {h.parameter: h for h in handlers}
        for parameter, value in instance_parameters.items():
            if parameter in handler_mapping:
                handler_mapping[parameter].on_received(instance=instance, value=value)

    dynamic_parameters_task.__name__ = 'dynamic'
    return dynamic_parameters_task


class DynamicParameters:
    all_apps = {}

    def __init__(self, riberry_workflow, handlers, beat_queue):
        self.workflow = riberry_workflow
        self.all_apps[self.workflow.app.main] = self
        self.workflow.app.task(name='dynamic-parameters')(make_dynamic_parameters_task(handlers=handlers))
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

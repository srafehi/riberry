import functools
from typing import Dict, AnyStr

import riberry


class RiberryApplicationBackend:
    default_stream_name = 'Overall'
    default_step_name = 'Entry'

    def __init__(self, instance):
        self.instance = instance

    def task(self, func=None, **options):
        if callable(func):
            return self.register_task(func=func, **options)
        else:
            return functools.partial(self.task, **options)

    def initialize(self):
        raise NotImplementedError

    def default_addons(self) -> Dict[AnyStr, 'riberry.app.addons.Addon']:
        raise NotImplementedError

    def register_task(self, func, **options):
        raise NotImplementedError

    def task_by_name(self, name: AnyStr):
        raise NotImplementedError

    def start_execution(self, execution_id, root_id, entry_point) -> AnyStr:
        raise NotImplementedError

    def create_receiver_task(self, external_task_id, validator):
        raise NotImplementedError

    def active_task(self):
        raise NotImplementedError

    def _execution_tracker(self):
        raise NotImplementedError

    @property
    def execution_tracker(self) -> 'riberry.app.backends.RiberryExecutionTracker':
        return self._execution_tracker()

    def __getattr__(self, item):
        return getattr(self.instance, item)

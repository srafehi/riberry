import enum
from collections import defaultdict
from functools import partial

import riberry


class EventRegistryTypes(enum.Enum):

    on_completion = 'on_completion'
    on_data_updated = 'on_data_update'
    on_report_refresh = 'on_report_refresh'
    on_external_result_received = 'on_external_result_received'


class EventRegistry:

    types: EventRegistryTypes = EventRegistryTypes

    def __init__(self, context):
        self.context: riberry.app.context.Context = context
        self._registrations = defaultdict(set)

    @staticmethod
    def _make_key(func, **key):
        if not key and func:
            return tuple([
                ('key', riberry.app.util.misc.function_path(func))
            ])
        return tuple(sorted(key.items()))

    def register(self, event_type: EventRegistryTypes, **key):
        def inner(func):
            formatted_key = self._make_key(func, **key)
            self._registrations[(event_type, formatted_key)].add(func)
            return func
        return inner

    def get(self, event_type: EventRegistryTypes, **key):
        key = self._make_key(func=None, **key)
        return self._registrations[(event_type, key)]

    def call(self, event_type: EventRegistryTypes, args=None, kwargs=None, **key):
        functions = self.get(event_type=event_type, **key)
        return [function(*args or (), **kwargs or {}) for function in functions]


class EventRegistryHelper:

    def __init__(self, context):
        self.context: riberry.app.context.Context = context

    @property
    def _register(self):
        return self.context.event_registry.register

    def _register_single_execution_function(self, func, event_type, key, registration_key=None):
        key = riberry.app.util.misc.internal_data_key(key=f'once.{key}')
        func = partial(self.context.data.execute_once, key=key, func=func)
        return self._register(event_type=event_type, **(registration_key or {}))(func)

    def execution_failed(self, func):
        return self._register_single_execution_function(
            func=func,
            event_type=EventRegistryTypes.on_completion,
            key='execution_failed',
            registration_key=dict(
                status='FAILURE',
            )
        )

    def execution_succeeded(self, func):
        return self._register_single_execution_function(
            func=func,
            event_type=EventRegistryTypes.on_completion,
            key='execution_succeeded',
            registration_key=dict(
                status='SUCCESS',
            )
        )

    def data_updated(self, name):
        return self._register(event_type=EventRegistryTypes.on_data_updated, data_name=name)

    def external_result_received(self, func):
        return self._register(event_type=EventRegistryTypes.on_external_result_received)(func)

    def report_refresh(self, report, bindings, renderer=None):
        def inner(func):
            def refresh():
                self.context.report.update(report=report, body=func(), renderer=renderer)
            self._register(event_type=EventRegistryTypes.on_report_refresh, report=report)(refresh)
            for binding in bindings:
                self._register(event_type=EventRegistryTypes.on_data_updated, data_name=binding)(refresh)
            return func
        return inner

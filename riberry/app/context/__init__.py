from contextlib import contextmanager

import riberry
from .artifact import Artifact
from .current import ContextCurrent
from .event_registry import EventRegistry, EventRegistryHelper
from .external_task import ExternalTask
from .flow import Flow
from .input_mapping import InputMappings
from .report import Report
from .shared_data import SharedExecutionData


class Context:

    def __init__(self):
        self.current = ContextCurrent(context=self)
        self.input = InputMappings(context=self)
        self.data = SharedExecutionData(context=self)
        self.flow = Flow(context=self)
        self.artifact = Artifact()
        self.report = Report(context=self)
        self.external_task = ExternalTask(context=self)
        self.event_registry = EventRegistry(context=self)
        self.on = EventRegistryHelper(context=self)

    @contextmanager
    def scope(self, root_id, task_id, stream, category, step):
        with self.current.scope(
            root_id=root_id,
            task_id=task_id,
            stream=stream,
            category=category,
            step=step,
        ):
            yield

    def spawn(self, form_name, job_name=None, input_values=None, input_files=None, owner=None, execute=True):
        return riberry.app.actions.jobs.create_job(
            form_name=form_name,
            job_name=job_name,
            input_files=input_files,
            input_values=input_values,
            owner=owner,
            execute=execute,
        )

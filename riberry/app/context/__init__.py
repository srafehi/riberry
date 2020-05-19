from contextlib import contextmanager
from typing import Union, Optional, Any

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
    def scope(self, root_id, task_id, task_name, stream, category, step):
        with self.current.scope(
            root_id=root_id,
            task_id=task_id,
            task_name=task_name,
            stream=stream,
            category=category,
            step=step,
        ):
            yield

    def spawn(
            self,
            form: Union[riberry.model.interface.Form, str],
            job_name: Optional[str] = None,
            input_data: Any = None,
            execute_on_creation: bool = False,
            owner: Optional[Union[riberry.model.auth.User, str]] = None,
    ) -> riberry.model.job.Job:
        return riberry.app.actions.jobs.create_job(
            form=form,
            job_name=job_name,
            input_data=input_data,
            execute_on_creation=execute_on_creation,
            owner=owner,
        )

"""
Defines the query authorizer which joins up to the allowed
Forms via the following dependency tree:

* Form
  * Application
  * ApplicationInstance
    * Heartbeat
    * ApplicationInstanceSchedule
  * InputValueDefinition
    * InputValueEnum
  * InputFileDefinition
  * Job
    * InputFileInstance
    * InputValueInstance
    * JobSchedule
    * JobExecution
      * JobExecutionReport
      * JobExecutionExternalTask
      * JobExecutionProgress
      * JobExecutionMetric
      * JobExecutionArtifact
        * JobExecutionArtifactBinary
        * JobExecutionArtifactData
      * JobExecutionStream
        * JobExecutionStreamStep
"""

from sqlalchemy.orm import Query

import riberry
from .base import StepResult, PermissionDomainQueryAuthorizer

form_authorizer = PermissionDomainQueryAuthorizer()

form_authorizer.register_chain(
    riberry.model.job.JobExecutionStreamStep,
    riberry.model.job.JobExecutionStream,
    riberry.model.job.JobExecution,
)

form_authorizer.register_chain(
    riberry.model.job.JobExecutionArtifactData,
    riberry.model.job.JobExecutionArtifact,
)

form_authorizer.register_chain(
    riberry.model.job.JobExecutionArtifactBinary,
    riberry.model.job.JobExecutionArtifact,
)

form_authorizer.register_chain(
    riberry.model.job.JobExecutionArtifact,
    riberry.model.job.JobExecution,
)

form_authorizer.register_chain(
    riberry.model.job.JobExecutionMetric,
    riberry.model.job.JobExecution,
)

form_authorizer.register_chain(
    riberry.model.job.JobExecutionProgress,
    riberry.model.job.JobExecution,
)

form_authorizer.register_chain(
    riberry.model.job.JobExecutionExternalTask,
    riberry.model.job.JobExecution,
)

form_authorizer.register_chain(
    riberry.model.job.JobExecutionReport,
    riberry.model.job.JobExecution,
)

form_authorizer.register_chain(
    riberry.model.job.JobExecution,
    riberry.model.job.Job,
)

form_authorizer.register_chain(
    riberry.model.job.JobSchedule,
    riberry.model.job.Job,
)

form_authorizer.register_chain(
    riberry.model.interface.InputValueInstance,
    riberry.model.job.Job,
)

form_authorizer.register_chain(
    riberry.model.interface.InputFileInstance,
    riberry.model.job.Job,
)

form_authorizer.register_chain(
    riberry.model.job.Job,
    riberry.model.interface.Form,
)

form_authorizer.register_chain(
    riberry.model.interface.InputFileDefinition,
    riberry.model.interface.Form,
)

form_authorizer.register_chain(
    riberry.model.interface.InputValueEnum,
    riberry.model.interface.InputValueDefinition,
    riberry.model.interface.Form,
)

form_authorizer.register_chain(
    riberry.model.application.Application,
    riberry.model.interface.Form,
)

form_authorizer.register_chain(
    riberry.model.application.ApplicationInstance,
    riberry.model.interface.Form,
)

form_authorizer.register_chain(
    riberry.model.application.ApplicationInstanceSchedule,
    riberry.model.application.ApplicationInstance,
)

form_authorizer.register_chain(
    riberry.model.application.Heartbeat,
    riberry.model.application.ApplicationInstance,
)


@form_authorizer.register_resolver(riberry.model.interface.Form)
def form_filter(query: Query, context):
    form_cls = riberry.model.interface.Form

    self_permission = f'{context.requested_permission}_SELF'
    if riberry.model.job.Job not in context.traversed:
        expression = form_cls.id.in_(
            context.permissions.get(context.requested_permission, set()) |
            context.permissions.get(self_permission, set())
        )
    else:
        expression = form_cls.id.in_(context.permissions.get(context.requested_permission, []))
        if context.permissions.get(self_permission):
            self_expression = form_cls.id.in_(context.permissions[self_permission])
            if riberry.model.job.Job in context.traversed:
                self_expression |= riberry.model.job.Job.creator_id == context.subject
            expression |= self_expression

    return StepResult(
        query,
        None,
        expression,
    )

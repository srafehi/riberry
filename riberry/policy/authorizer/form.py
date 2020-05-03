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
from .base import StepResult, PermissionDomainQueryAuthorizer, Node

form_authorizer = PermissionDomainQueryAuthorizer()

_node_tree = Node(riberry.model.interface.Form, (
    Node(riberry.model.application.Application),
    Node(riberry.model.application.ApplicationInstance, (
        Node(riberry.model.application.Heartbeat),
        Node(riberry.model.application.ApplicationInstanceSchedule),
    )),
    Node(riberry.model.interface.InputValueDefinition, (
        Node(riberry.model.interface.InputValueEnum),
    )),
    Node(riberry.model.interface.InputFileDefinition),
    Node(riberry.model.job.Job, (
        Node(riberry.model.interface.InputFileInstance),
        Node(riberry.model.interface.InputValueInstance),
        Node(riberry.model.job.JobSchedule),
        Node(riberry.model.job.JobExecution, (
            Node(riberry.model.job.JobExecutionReport),
            Node(riberry.model.job.JobExecutionExternalTask),
            Node(riberry.model.job.JobExecutionProgress),
            Node(riberry.model.job.JobExecutionMetric),
            Node(riberry.model.job.JobExecutionStream, (
                Node(riberry.model.job.JobExecutionStreamStep),
            )),
            Node(riberry.model.job.JobExecutionArtifact, (
                Node(riberry.model.job.JobExecutionArtifactBinary),
                Node(riberry.model.job.JobExecutionArtifactData),
            )),
        )),
    ))
))

form_authorizer.register_node(node=_node_tree)


@form_authorizer.register_resolver(riberry.model.interface.Form)
def form_filter(query: Query, context):
    form_cls = riberry.model.interface.Form

    self_permission = f'{context.requested_permission}_SELF'
    select_entities = {d['entity'] for d in query.column_descriptions}

    form_ids = context.permissions.get(context.requested_permission, set())
    self_form_ids = context.permissions.get(self_permission, set())

    if riberry.model.job.Job not in context.traversed | select_entities:
        expression = form_cls.id.in_(form_ids | self_form_ids)
    else:
        expression = form_cls.id.in_(form_ids) | (
            form_cls.id.in_(self_form_ids) &
            (riberry.model.job.Job.creator_id == context.subject.id)
        )

    return StepResult(
        query,
        None,
        expression,
    )
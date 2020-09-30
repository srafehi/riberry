"""
Defines the query authorizer which joins up to the allowed
Forms via the following dependency tree:

* Form
  * Application
  * ApplicationInstance
    * Heartbeat
    * ApplicationInstanceSchedule
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
from sqlalchemy import inspect
from sqlalchemy.orm import Query

import riberry
from .base import StepResult, PermissionDomainQueryAuthorizer, Node, QueryAuthorizerContext

form_authorizer = PermissionDomainQueryAuthorizer()


def job_form_joiner(query: Query, context: QueryAuthorizerContext) -> Query:
    """ TODO """
    return query.filter(riberry.model.job.Job.form_id == riberry.model.interface.Form.id)


def check_job_prioritization_access(query: Query, context: QueryAuthorizerContext) -> Query:
    """ Ensures that any change to JobExecution.priority is made only by those who
    have the permissions to do so. """

    if (
        context.target_entities and
        context.source_model == riberry.model.job.JobExecution and
        context.requested_permission == riberry.policy.permissions.FormDomain.PERM_JOB_PRIORITIZE and
        riberry.policy.permissions.FormDomain.PERM_JOB_PRIORITIZE not in context.permissions
    ):
        invalid_ids = None

        # ensure created executions have not had their priorities changed
        if context.requested_operation == riberry.policy.permissions.crud.CREATE:
            default_value = riberry.model.helpers.default_value(riberry.model.job.JobExecution.priority)
            invalid_ids = {
                entity.id for entity in context.target_entities
                if entity.priority != default_value
            }

        # ensure updated executions have not had their priorities changed
        elif context.requested_operation == riberry.policy.permissions.crud.UPDATE:
            invalid_ids = {
                entity.id for entity in context.target_entities
                if inspect(entity).attrs.priority.load_history().added
            }

        # invalidate the query if any invalid changes to priority have been made
        if invalid_ids:
            return query.filter(riberry.model.job.JobExecution.id.notin_(invalid_ids))

    return query


_node_tree = Node(riberry.model.interface.Form, (
    Node(riberry.model.application.Application),
    Node(riberry.model.application.ApplicationInstance, (
        Node(riberry.model.application.Heartbeat),
        Node(riberry.model.application.ApplicationInstanceSchedule),
    )),
    Node(riberry.model.interface.InputDefinition),
    Node(riberry.model.job.Job, joiner=job_form_joiner, dependents=(
        Node(riberry.model.interface.InputFileInstance),
        Node(riberry.model.interface.InputValueInstance),
        Node(riberry.model.job.JobSchedule),
        Node(riberry.model.job.JobExecution, processor=check_job_prioritization_access, dependents=(
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

    if riberry.model.job.Job in context.traversed | select_entities:
        expression = form_cls.id.in_(form_ids) | (
                form_cls.id.in_(self_form_ids) &
                (riberry.model.job.Job.creator_id == context.subject.id)
        )
    else:
        expression = form_cls.id.in_(form_ids | self_form_ids)

    if riberry.model.application.ApplicationInstanceSchedule in select_entities:
        if context.requested_permission == riberry.policy.permissions.FormDomain.PERM_APP_SCHEDULES_READ_BUILTIN:
            expression &= riberry.model.application.ApplicationInstanceSchedule.parameter.in_([
                'active'  # TODO define all "built-in" schedules
            ])
        elif context.requested_permission != riberry.policy.permissions.FormDomain.PERM_APP_SCHEDULES_READ:
            expression &= riberry.model.application.ApplicationInstanceSchedule.parameter.in_([])

    return StepResult(
        query,
        None,
        expression,
    )

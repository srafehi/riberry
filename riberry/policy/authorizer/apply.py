import functools
import operator
from collections import defaultdict
from typing import List, Optional

from sqlalchemy import inspect
from sqlalchemy.orm import Query, Session

import riberry
from riberry.typing import ModelType, Model
from .application import application_authorizer
from .base import QueryAuthorizerContext
from .form import form_authorizer

log = riberry.log.make(__name__)


def starting_models(query: Query, selected_model=None) -> ModelType:
    yield from (
        desc['entity']
        for desc in query.column_descriptions
        if desc['entity'] is not None
        if (desc['entity'] == selected_model if selected_model else True)
    )


def apply_auth_for_session_states(session: Session, *states: str):
    mapping = {
        'new': riberry.policy.permissions.crud.CREATE,
        'dirty': riberry.policy.permissions.crud.UPDATE,
        'deleted': riberry.policy.permissions.crud.DELETE,
    }

    model_state_mappings = defaultdict(lambda: defaultdict(set))
    for state in states:
        for item in getattr(session, state):
            if state == 'deleted' or session.is_modified(item, include_collections=False):
                model_state_mappings[type(item)][mapping[state]].add(item)

    for model_type, state_mappings in model_state_mappings.items():
        for state, entities in state_mappings.items():
            riberry.policy.authorizer.apply_auth_for_entities(model_type, entities, state)


def custom_filters(query: Query, context: QueryAuthorizerContext) -> Query:
    # TODO(Shady) considering pushing down to authorizers
    if (
        context.source_model == riberry.model.job.JobExecution and
        context.requested_permission == riberry.policy.permissions.FormDomain.PERM_JOB_PRIORITIZE and
        riberry.policy.permissions.FormDomain.PERM_JOB_PRIORITIZE not in context.permissions and
        context.target_entities
    ):
        invalid_ids = None
        if context.requested_operation == riberry.policy.permissions.crud.CREATE:
            default_value = riberry.model.helpers.default_value(riberry.model.job.JobExecution.priority)
            invalid_ids = {
                entity.id for entity in context.target_entities
                if entity.priority != default_value
            }
        elif context.requested_operation == riberry.policy.permissions.crud.UPDATE:
            invalid_ids = {
                entity.id for entity in context.target_entities
                if inspect(entity).attrs.priority.load_history().added
            }
        if invalid_ids:
            return query.filter(riberry.model.job.JobExecution.id.notin_(invalid_ids))

    return query


def apply_auth_for_entities(model_type, entities: List, state: str):
    ids = {e.id for e in entities}
    query = riberry.model.conn.query(model_type.id).filter(model_type.id.in_(ids))
    new_query = apply_auth_to_query(query, state, entities=entities)
    with riberry.policy.context.disabled_scope():
        new_ids = {id_ for (id_,) in new_query.all()}
    if ids != new_ids:
        raise riberry.exc.AuthorizationError(model_type=model_type, state=state)


def apply_auth_to_query(
        query: Query,
        state: str,
        starting_model: Optional[ModelType] = None,
        entities: Optional[List[Model]] = None,
) -> Query:
    if not riberry.policy.context.enabled:
        return query

    subject = riberry.policy.context.subject
    with riberry.policy.context.disabled_scope():
        for model_type in starting_models(query=query, selected_model=starting_model):
            try:
                permissions = riberry.policy.permissions.crud.CRUD_PERMISSIONS[model_type][state] | {
                    riberry.policy.permissions.SystemDomain.PERM_ACCESS
                }
            except KeyError:
                log.warning(f'State {state!r} unsupported for model {model_type.__name__!r}')
                continue

            user_permissions = subject.permissions_to_domain_ids()

            if riberry.policy.permissions.SystemDomain.PERM_ACCESS not in user_permissions:
                permissions.remove(riberry.policy.permissions.SystemDomain.PERM_ACCESS)
                if not permissions:
                    continue
                else:
                    expressions = []
                    permissions -= {riberry.policy.permissions.SystemDomain.PERM_ACCESS}
                    for permission in permissions:
                        if permission in riberry.policy.permissions.FormDomain:
                            authorizer = form_authorizer
                        elif permission in riberry.policy.permissions.ApplicationDomain:
                            authorizer = application_authorizer
                        else:
                            continue
                        context = QueryAuthorizerContext(
                            subject=subject,
                            requested_permission=permission,
                            requested_operation=state,
                            source_model=model_type,
                            target_entities=entities,
                        )
                        query = custom_filters(query, context)
                        query, expression = authorizer.apply_filter(query.enable_assertions(False), context=context)
                        if expression is not None:
                            expressions.append(expression)

                    if expressions:
                        query = query.filter(functools.reduce(operator.or_, expressions))
        return query.enable_assertions(True)

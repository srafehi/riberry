import functools
import operator
from typing import List, Optional

from sqlalchemy.orm import Query

import riberry
from riberry.typing import ModelType
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


def apply_auth_for_entities(model_type, entities: List, state: str):
    ids = {e.id for e in entities}
    query = riberry.model.conn.query(model_type.id).filter(model_type.id.in_(ids))
    new_query = apply_auth_to_query(query, state)
    with riberry.policy.context.disabled_scope():
        new_ids = {id_ for (id_,) in new_query.all()}
    if ids != new_ids:
        raise Exception(f'{model_type.__name__}[{state!r}]: {ids} -> {new_ids}')


def apply_auth_to_query(query: Query, state: str, starting_model: Optional[ModelType] = None) -> Query:
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
                        context = QueryAuthorizerContext(subject, permission)
                        query, expression = authorizer.apply_filter(query.enable_assertions(False), context=context)
                        if expression is not None:
                            expressions.append(expression)

                    if expressions:
                        query = query.filter(functools.reduce(operator.or_, expressions))
        return query.enable_assertions(True)

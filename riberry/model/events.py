from collections import defaultdict

from sqlalchemy.orm import Query, Session

import riberry


def before_flush(session, *_):
    if riberry.policy.context.enabled:
        riberry.policy.authorizer.apply_auth_for_session_states(session, 'deleted')


def after_flush(session, *_):
    if riberry.policy.context.enabled:
        riberry.policy.authorizer.apply_auth_for_session_states(session, 'new', 'dirty')


def before_compile(query: Query):
    if riberry.policy.context.enabled:
        query = riberry.policy.authorizer.apply_auth_to_query(query, riberry.policy.permissions.crud.READ)
    return query.execution_options(_dynamic=True)

from collections import defaultdict

from sqlalchemy.orm import Query

import riberry


def after_flush(session, _flush_context):
    mapping = {
        'new': riberry.policy.permissions.crud.CREATE,
        'dirty': riberry.policy.permissions.crud.UPDATE,
        'deleted': riberry.policy.permissions.crud.DELETE,
    }

    model_state_mappings = defaultdict(lambda: defaultdict(set))
    for state in 'new', 'dirty', 'deleted':
        for item in getattr(session, state):
            if session.is_modified(item, include_collections=False):
                model_state_mappings[type(item)][mapping[state]].add(item)

    for model_type, state_mappings in model_state_mappings.items():
        for state, entities in state_mappings.items():
            riberry.policy.authorizer.apply_auth_for_entities(model_type, entities, state)


def before_compile(query: Query):
    if riberry.policy.context.enabled:
        query = riberry.policy.authorizer.apply_auth_to_query(query, riberry.policy.permissions.crud.READ)
    return query.execution_options(_dynamic=True)

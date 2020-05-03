from contextlib import contextmanager

from riberry import model, config, policy


@contextmanager
def policy_scope(user=None, environment=None):
    if isinstance(user, str):
        user = model.auth.User.query().filter_by(username=user).one()

    with policy.context.scope(subject=user, environment=environment):
        yield

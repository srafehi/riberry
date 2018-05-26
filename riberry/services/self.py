from riberry import model, policy


def profile() -> model.auth.User:
    return policy.context.subject
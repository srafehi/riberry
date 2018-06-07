from sqlalchemy import desc

from riberry import model, policy


def profile() -> model.auth.User:
    return policy.context.subject


def latest_notifications():
    return model.misc.UserNotification.query().order_by(
        desc(model.misc.UserNotification.created)
    ).limit(32).all()


def unread_notifications():
    user = policy.context.subject
    return model.misc.UserNotification.query().filter_by(read=False, user=user).count()
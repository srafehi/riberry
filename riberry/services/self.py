from typing import List

from sqlalchemy import desc

from riberry import model, policy


def profile() -> model.auth.User:
    return policy.context.subject


def latest_notifications():
    user = policy.context.subject
    return model.misc.UserNotification.query().filter_by(user=user).order_by(
        desc(model.misc.UserNotification.created)
    ).limit(32).all()


def unread_notification_count():
    user = policy.context.subject
    return model.misc.UserNotification.query().filter_by(read=False, user=user).count()


def mark_notifications_as_read(notification_ids: List):
    user = policy.context.subject
    notifications: List[model.misc.UserNotification] = model.misc.UserNotification.query().filter(
        (model.misc.UserNotification.id.in_(notification_ids)) &
        (model.misc.UserNotification.user == user) &
        (model.misc.UserNotification.read == False)
    ).all()

    for notification in notifications:
        notification.read = True


def mark_all_notifications_as_read():
    user = policy.context.subject
    notifications: List[model.misc.UserNotification] = model.misc.UserNotification.query().filter_by(
        user=user,
        read=False
    ).all()

    for notification in notifications:
        notification.read = True


from typing import Tuple

from riberry import model, services
from sqlalchemy.orm.exc import NoResultFound


def _find_user_and_group(username: str, group_name: str) -> Tuple[model.auth.User, model.group.Group]:
    try:
        user = model.auth.User.query().filter_by(username=username).one()
    except NoResultFound:
        raise ValueError(f'Could not find user with username {username!r}')

    try:
        group = model.group.Group.query().filter_by(name=group_name).one()
    except NoResultFound:
        raise ValueError(f'Could not find group with name {group_name!r}')

    return user, group


def add_user_to_group(username: str, group_name: str):
    user, group = _find_user_and_group(username=username, group_name=group_name)
    services.auth.add_user_to_group(group_id=group.id, user_id=user.id)
    model.conn.commit()


def remove_user_from_group(username: str, group_name: str):
    user, group = _find_user_and_group(username=username, group_name=group_name)
    services.auth.remove_user_from_group(group_id=group.id, user_id=user.id)
    model.conn.commit()

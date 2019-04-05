from riberry import model
from datetime import timedelta
from riberry.model.group import Group


def authenticate_user(username: str, password: str) -> str:
    user = model.auth.User.authenticate(username=username, password=password)
    access_token = model.auth.AuthToken.create(user, expiry_delta=timedelta(days=5))
    return access_token.decode()


def all_groups():
    return Group.query().all()


def group_by_id(group_id):
    return Group.query().filter_by(id=group_id).first()


def users_for_group_id(group_id):
    group = group_by_id(group_id=group_id)
    return group.users


def forms_for_group_id(group_id):
    group = group_by_id(group_id=group_id)
    return group.forms


def remove_user_from_group(group_id, user_id):
    _remove_resource_from_group(group_id, user_id, model.misc.ResourceType.user)


def add_user_to_group(group_id, user_id):
    _add_resource_to_group(group_id, user_id, model.misc.ResourceType.user)


def remove_form_from_group(group_id, form_id):
    _remove_resource_from_group(group_id, form_id, model.misc.ResourceType.form)


def add_form_to_group(group_id, form_id):
    _add_resource_to_group(group_id, form_id, model.misc.ResourceType.form)


def _find_group_association(group_id, resource_id, resource_type):
    return model.group.ResourceGroupAssociation.query().filter_by(
        group_id=group_id,
        resource_id=resource_id,
        resource_type=resource_type
    ).first()


def _remove_resource_from_group(group_id, resource_id, resource_type):
    association = _find_group_association(group_id, resource_id, resource_type)
    if association:
        model.conn.delete(association)


def _add_resource_to_group(group_id, resource_id, resource_type):
    association = _find_group_association(group_id, resource_id, resource_type)
    if not association:
        association = model.group.ResourceGroupAssociation(
            group_id=group_id,
            resource_id=resource_id,
            resource_type=resource_type
        )
        model.conn.add(association)


def create_group(name):
    group = model.group.Group(name=name)
    model.conn.add(group)
    return group
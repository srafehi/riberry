from riberry import model
from datetime import timedelta
from riberry.model.group import Group


def authenticate_user(username: str, password: str) -> str:
    user = model.auth.User.authenticate(username=username, password=password)
    access_token = model.auth.AuthToken.create(user, expiry_delta=timedelta(days=1))
    return access_token.decode()


def all_groups():
    return Group.query().all()


def group_by_id(group_id):
    return Group.query().filter_by(id=group_id).first()


def users_for_group_id(group_id):
    group = group_by_id(group_id=group_id)
    return group.users


def instance_interfaces_for_group_id(group_id):
    group = group_by_id(group_id=group_id)
    return group.instance_interfaces


def remove_user_from_group(group_id, user_id):
    _remove_resource_from_group(group_id, user_id, model.group.ResourceType.user)


def add_user_from_group(group_id, user_id):
    _add_resource_from_group(group_id, user_id, model.group.ResourceType.user)


def remove_instance_interface_from_group(group_id, instance_interface_id):
    _remove_resource_from_group(group_id, instance_interface_id, model.group.ResourceType.application_instance_interface)


def add_instance_interface_from_group(group_id, instance_interface_id):
    _add_resource_from_group(group_id, instance_interface_id, model.group.ResourceType.application_instance_interface)


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
        model.conn.commit()


def _add_resource_from_group(group_id, resource_id, resource_type):
    association = _find_group_association(group_id, resource_id, resource_type)
    if not association:
        association = model.group.ResourceGroupAssociation(
            group_id=group_id,
            resource_id=resource_id,
            resource_type=resource_type
        )
        model.conn.add(association)
        model.conn.commit()


def create_group(name):
    group = model.group.Group(name=name)
    model.conn.add(group)
    model.conn.commit()
    return group
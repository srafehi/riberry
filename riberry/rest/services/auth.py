from typing import Dict

from riberry import services
from riberry.rest import view_models


def authenticate_user(username: str, password: str) -> Dict:
    access_token = services.auth.authenticate_user(username=username, password=password)
    return {
        'token': access_token
    }


def all_groups(options=None):
    groups = services.auth.all_groups()
    return [view_models.Group(model=group, options=options).to_dict() for group in groups]


def group_by_id(group_id, options=None):
    group = services.auth.group_by_id(group_id=group_id)
    return view_models.Group(model=group, options=options).to_dict()


def users_for_group_id(group_id, options=None):
    users = services.auth.users_for_group_id(group_id=group_id)
    return [view_models.User(model=user, options=options).to_dict() for user in users]


def forms_for_group_id(group_id, options=None):
    users = services.auth.forms_for_group_id(group_id=group_id)
    return [view_models.Form(model=user, options=options).to_dict() for user in users]


def remove_user_from_group(group_id, user_id):
    return services.auth.remove_user_from_group(group_id, user_id)


def remove_form_from_group(group_id, form_id):
    return services.auth.remove_form_from_group(group_id, form_id)


def add_user_to_group(group_id: object, user_id: object):
    return services.auth.add_user_to_group(group_id, user_id)


def add_form_to_group(group_id, form_id):
    return services.auth.add_form_to_group(group_id, form_id)


def create_group(name):
    group = services.auth.create_group(name=name)
    return view_models.Group(model=group, options=None).to_dict()
from typing import List

from riberry import model, policy
from riberry import services


@policy.context.post_filter(action='view')
def all_forms() -> List[model.interface.Form]:
    return model.interface.Form.query().all()


@policy.context.post_authorize(action='view')
def form_by_id(form_id) -> model.interface.Form:
    return model.interface.Form.query().filter_by(id=form_id).one()


def create_form(instance_id, interface_id, group_names) -> model.interface.Form:
    instance = services.application_instance.application_instance_by_id(application_instance_id=instance_id)
    interface = services.application_interface.application_interface_by_id(application_interface_id=interface_id)

    assert instance.application.id == interface.application.id, \
        'Instance and interface do not belong to the same application'

    groups = []
    for group_name in group_names:
        group = model.group.Group.query().filter_by(name=group_name).one()
        groups.append(group)

    form = model.interface.Form(
        instance=instance,
        interface=interface
    )

    policy.context.authorize(form, action='create')

    for group in groups:
        group_association = model.group.ResourceGroupAssociation(
            resource_id=group.id,
            resource_type=model.group.ResourceType.form,
            group=group
        )
        model.conn.add(group_association)

    model.conn.add(form)
    model.conn.commit()
    return form

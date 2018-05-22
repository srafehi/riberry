from typing import List

from riberry import model, policy
from riberry import services


@policy.context.post_filter(action='view')
def all_instance_interfaces() -> List[model.interface.ApplicationInstanceInterface]:
    return model.interface.ApplicationInstanceInterface.query().all()


@policy.context.post_authorize(action='view')
def instance_interface_by_id(instance_interface_id) -> model.interface.ApplicationInstanceInterface:
    return model.interface.ApplicationInstanceInterface.query().filter_by(id=instance_interface_id).one()


def create_instance_interface(instance_id, interface_id, group_names) -> model.interface.ApplicationInstanceInterface:
    instance = services.application_instance.application_instance_by_id(application_instance_id=instance_id)
    interface = services.application_interface.application_interface_by_id(application_interface_id=interface_id)

    assert instance.application.id == interface.application.id, \
        'Instance and interface do not belong to the same application'

    groups = []
    for group_name in group_names:
        group = model.group.Group.query().filter_by(name=group_name).one()
        groups.append(group)

    instance_interface = model.interface.ApplicationInstanceInterface(
        instance=instance,
        interface=interface
    )

    policy.context.authorize(instance_interface, action='create')

    for group in groups:
        group_association = model.group.ResourceGroupAssociation(
            resource_id=group.id,
            resource_type=model.group.ResourceType.application_instance_interface,
            group=group
        )
        model.conn.add(group_association)


    model.conn.add(instance_interface)
    model.conn.commit()
    return instance_interface

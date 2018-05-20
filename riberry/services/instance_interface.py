from typing import List

from riberry import model, policy


@policy.context.post_filter(action='view')
def all_instance_interfaces() -> List[model.interface.ApplicationInstanceInterface]:
    return model.interface.ApplicationInstanceInterface.query().all()


@policy.context.post_authorize(action='view')
def instance_interface_by_id(instance_interface_id) -> model.interface.ApplicationInstanceInterface:
    return model.interface.ApplicationInstanceInterface.query().filter_by(id=instance_interface_id).one()

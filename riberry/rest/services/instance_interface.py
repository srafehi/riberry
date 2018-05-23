from typing import List, Dict
from riberry import services
from riberry.rest import view_models


def all_instance_interfaces(options) -> List[Dict]:
    instance_interfaces = services.instance_interface.all_instance_interfaces()
    return [view_models.ApplicationInstanceInterface(model=app, options=options).to_dict() for app in instance_interfaces]


def instance_interface_by_id(instance_interface_id, options) -> Dict:
    instance_interface = services.instance_interface.instance_interface_by_id(instance_interface_id)
    return view_models.ApplicationInstanceInterface(model=instance_interface, options=options).to_dict()


def create_instance_interface(instance_id, interface_id, groups) -> Dict:
    instance_interface = services.instance_interface.create_instance_interface(instance_id, interface_id, groups)
    return view_models.ApplicationInstanceInterface(model=instance_interface, options=None).to_dict()

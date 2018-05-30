import json
from typing import List, Dict

from riberry import services
from riberry.rest import view_models


def all_application_interfaces(options) -> List[Dict]:
    application_interfaces = services.application_interface.all_application_interfaces()
    return [view_models.ApplicationInterface(model=app, options=options).to_dict() for app in application_interfaces]


def application_interface_by_id(application_interface_id, options) -> Dict:
    application_interface = services.application_interface.\
        application_interface_by_id(application_interface_id=application_interface_id)
    return view_models.ApplicationInterface(model=application_interface, options=options).to_dict()


def interfaces_by_application_id(application_id, options) -> List[Dict]:
    interfaces = services.application_interface.interfaces_by_application_id(application_id=application_id)
    return [view_models.ApplicationInterface(model=interface, options=options).to_dict() for interface in interfaces]


def _cleanse_input_definitions(data):
    data = dict(data)

    data['internal_name'] = data['internalName']
    del data['internalName']

    if 'defaults' in data:
        data['default_binary'] = json.dumps(data['defaults']).encode()
        del data['defaults']

    if 'enumerations' in data:
        data['allowed_binaries'] = [json.dumps(e).encode() for e in data['enumerations']]
        del data['enumerations']

    return data


def create_application_interface(application_id, name, internal_name, version, description, input_files, input_values):
    application_interface = services.application_interface.create_application_interface(
        application_id=application_id,
        name=name,
        internal_name=internal_name,
        version=version,
        description=description,
        input_files=[_cleanse_input_definitions(d) for d in input_files],
        input_values=[_cleanse_input_definitions(d) for d in input_values]
    )
    return view_models.ApplicationInterface(model=application_interface, options=None).to_dict()

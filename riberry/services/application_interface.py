from typing import List

from riberry import model, policy, services


@policy.context.post_filter(action='view')
def all_application_interfaces() -> List[model.application.ApplicationInstance]:
    return model.interface.ApplicationInterface.query().all()


@policy.context.post_authorize(action='view')
def application_interface_by_id(application_interface_id) -> model.interface.ApplicationInterface:
    return model.interface.ApplicationInterface.query().filter_by(id=application_interface_id).one()


@policy.context.post_filter(action='view')
def interfaces_by_application_id(application_id):
    application = services.application.application_by_id(application_id=application_id)
    return application.interfaces


def create_application_interface(application_id, name, internal_name, version, description, input_files, input_values):
    application = services.application.application_by_id(application_id=application_id)
    application_interface = model.interface.ApplicationInterface(
        application=application,
        name=name,
        internal_name=internal_name,
        version=version,
        description=description,
        input_file_definitions=[model.interface.InputFileDefinition(**d) for d in input_files],
        input_value_definitions=[model.interface.InputValueDefinition(**d) for d in input_values]
    )

    policy.context.authorize(application_interface, action='create')
    model.conn.add(application_interface)
    model.conn.commit()
    return application_interface

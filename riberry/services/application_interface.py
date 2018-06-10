from typing import List, Dict

from riberry import model, policy, services


@policy.context.post_filter(action='view')
def all_application_interfaces() -> List[model.application.ApplicationInstance]:
    return model.interface.ApplicationInterface.query().all()


@policy.context.post_authorize(action='view')
def application_interface_by_id(application_interface_id) -> model.interface.ApplicationInterface:
    return model.interface.ApplicationInterface.query().filter_by(id=application_interface_id).one()


@policy.context.post_authorize(action='view')
def application_interface_by_internal_name(internal_name) -> model.interface.ApplicationInterface:
    return model.interface.ApplicationInterface.query().filter_by(internal_name=internal_name).one()


@policy.context.post_filter(action='view')
def interfaces_by_application_id(application_id):
    application = services.application.application_by_id(application_id=application_id)
    return application.interfaces


def create_application_interface(application, name, internal_name, version, description, input_files, input_values):
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
    return application_interface


def update_application_interface(interface: model.interface.ApplicationInterface, attributes: Dict):
    for attr in {'name', 'description'} & set(attributes):
        setattr(interface, attr, attributes[attr])
    return interface


@policy.context.post_authorize(action='view')
def file_definition_by_internal_name(interface, internal_name) -> model.interface.InputFileDefinition:
    return model.interface.InputFileDefinition.query().filter_by(
        interface=interface,
        internal_name=internal_name,
    ).one()


@policy.context.post_authorize(action='view')
def value_definition_by_internal_name(interface, internal_name) -> model.interface.InputValueDefinition:
    return model.interface.InputValueDefinition.query().filter_by(
        interface=interface,
        internal_name=internal_name,
    ).one()


def update_file_definition(definition: model.interface.InputFileDefinition, attributes: Dict):
    form_ids = [f.id for f in definition.interface.forms]
    has_jobs = bool(model.job.Job.query().filter(model.job.Job.form_id.in_(form_ids)).count())

    for attr in {'required', 'type', 'name', 'description'} & set(attributes):
        if has_jobs and attr in {'required', 'type'}:
            if attributes[attr] != getattr(definition, attr):
                raise Exception(f'Cannot change value {attr!r} for interface input which already has jobs')
        setattr(definition, attr, attributes[attr])

    return definition


def update_value_definition(definition: model.interface.InputValueDefinition, attributes: Dict):
    form_ids = [f.id for f in definition.interface.forms]
    has_jobs = bool(model.job.Job.query().filter(model.job.Job.form_id.in_(form_ids)).count())

    for attr in {'required', 'type', 'name', 'description', 'default_binary'} & set(attributes):
        if has_jobs and attr in {'required', 'type', 'default_binary'}:
            if attributes[attr] != getattr(definition, attr):
                raise Exception(f'Cannot change value {attr!r} for interface input which already has jobs')
        setattr(definition, attr, attributes[attr])

    if 'allowed_binaries' in attributes:
        if has_jobs and tuple(attributes['allowed_binaries']) != tuple(definition.allowed_binaries):
            raise Exception(f'Cannot change enumerations for interface input which already has jobs')
        else:
            for enum in definition.allowed_value_enumerations:
                model.conn.delete(enum)
            definition.allowed_binaries = list(attributes['allowed_binaries'])

    return definition

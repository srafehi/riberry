from typing import Dict

from riberry import model, services, policy
import json


def jobs_by_instance_interface_id(instance_interface_id):
    return model.job.Job.query().filter_by(instance_interface_id=instance_interface_id).all()


def verify_inputs(input_value_definitions, input_file_definitions, input_values, input_files):
    value_map_definitions: Dict[str, 'model.interface.InputValueDefinition'] = {input_def.name: input_def for input_def in input_value_definitions}
    file_map_definitions: Dict[str, 'model.interface.InputValueDefinition'] = {input_def.name: input_def for input_def in input_file_definitions}

    input_values = dict(input_values)
    input_files = dict(input_files)

    input_value_mapping = {}
    input_file_mapping = {}

    for name, definition in value_map_definitions.items():
        if name in input_values:
            value = input_values.pop(name)
        else:
            value = definition.default_binary

        if definition.required and not value:
            raise ValueError(f'Mandatory input {repr(definition.name)}/{repr(definition.internal_name)} not provided')
        if definition.allowed_values and value not in definition.allowed_binaries:
            raise ValueError(
                f'Input {repr(definition.name)}/{repr(definition.internal_name)} provided invalid enumeration: {value} '
                f'(expected: {definition.allowed_binaries})')

        input_value_mapping[definition] = value

    for name, definition in file_map_definitions.items():
        if name in input_files:
            value = input_files.pop(name)
        else:
            value = None

        if definition.required and not value:
            raise ValueError(f'Mandatory file {repr(definition.name)}/{repr(definition.internal_name)} not provided')

        input_file_mapping[definition] = value

    unexpected_inputs = set(input_values) | set(input_files)
    if unexpected_inputs:
        raise ValueError(f'Received unexpected arguments: {unexpected_inputs}')

    return input_value_mapping, input_file_mapping


def create_job(instance_interface_id, name, input_values, input_files):
    input_values = {k: (json.dumps(v).encode() if v else v) for k, v in input_values.items()}
    instance_interface = services.instance_interface.instance_interface_by_id(
        instance_interface_id=instance_interface_id)
    policy.context.authorize(instance_interface, action='view')

    input_file_definitions = instance_interface.interface.input_file_definitions
    input_value_definitions = instance_interface.interface.input_value_definitions

    values_mapping, files_mapping = verify_inputs(
        input_value_definitions,
        input_file_definitions,
        input_values,
        input_files
    )

    input_value_instances = []
    input_file_instances = []

    for definition, value in values_mapping.items():
        input_value_instance = model.interface.InputValueInstance(
            definition=definition,
            raw_value=value
        )
        input_value_instances.append(input_value_instance)

    for definition, value in files_mapping.items():
        binary = value.read()
        filename = value.filename or definition.internal_name
        input_file_instance = model.interface.InputFileInstance(
            definition=definition,
            filename=filename,
            binary=binary,
            size=len(binary) if binary else 0
        )
        input_file_instances.append(input_file_instance)

    job = model.job.Job(
        instance_interface=instance_interface,
        name=name,
        files=input_file_instances,
        values=input_value_instances,
        creator=policy.context.subject
    )

    policy.context.authorize(job, action='create')

    model.conn.add(job)
    model.conn.commit()

    return job
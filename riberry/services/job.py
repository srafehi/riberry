from typing import Dict

from riberry import model, services, policy
import json


def jobs_by_form_id(form_id):
    return model.job.Job.query().filter_by(form_id=form_id).all()


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


def create_job(form_id, name, input_values, input_files):
    input_values = {k: (json.dumps(v).encode() if v else v) for k, v in input_values.items()}
    form = services.form.form_by_id(form_id=form_id)
    policy.context.authorize(form, action='view')

    input_file_definitions = form.interface.input_file_definitions
    input_value_definitions = form.interface.input_value_definitions

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
        form=form,
        name=name,
        files=input_file_instances,
        values=input_value_instances,
        creator=policy.context.subject
    )

    policy.context.authorize(job, action='create')

    model.conn.add(job)
    model.conn.commit()

    return job


@policy.context.post_authorize(action='view')
def job_by_id(job_id):
    return model.job.Job.query().filter_by(id=job_id).one()


@policy.context.post_filter(action='view')
def job_executions_by_id(job_id):
    return model.job.JobExecution.query().filter_by(job_id=job_id).all()


def create_job_execution(job_id):
    job = job_by_id(job_id=job_id)
    execution = model.job.JobExecution(job=job)

    policy.context.authorize(execution, action='create')
    model.conn.add(execution)
    model.conn.commit()

    return execution

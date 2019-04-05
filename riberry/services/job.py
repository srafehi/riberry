import json
from io import BytesIO
from typing import Dict

from riberry import model, services, policy, exc


class InputFileProxy(BytesIO):

    def __init__(self, obj, filename=None):
        self.filename = filename
        super().__init__(obj)

    @classmethod
    def from_object(cls, obj, filename='input'):
        if isinstance(obj, bytes):
            binary = obj
        elif isinstance(obj, str):
            binary = obj.encode()
            if not filename.endswith('.txt'):
                filename += '.txt'
        else:
            binary = json.dumps(obj).encode()
            if not filename.endswith('.json'):
                filename += '.json'

        return cls(binary, filename)


def jobs_by_form_id(form_id):
    return model.job.Job.query().filter_by(form_id=form_id).all()


def verify_inputs(input_value_definitions, input_file_definitions, input_values, input_files):
    value_map_definitions: Dict[str, 'model.interface.InputValueDefinition'] = {input_def.name: input_def for input_def in input_value_definitions}
    file_map_definitions: Dict[str, 'model.interface.InputValueDefinition'] = {input_def.name: input_def for input_def in input_file_definitions}

    value_mapping = {d.internal_name: d.name for d in value_map_definitions.values()}
    file_mapping = {d.internal_name: d.name for d in file_map_definitions.values()}

    input_values = {value_mapping.get(k, k): v for k, v in input_values.items()}
    input_files = {file_mapping.get(k, k): v for k, v in input_files.items()}

    input_value_mapping = {}
    input_file_mapping = {}
    errors = []

    for name, definition in value_map_definitions.items():
        if name in input_values:
            value = input_values.pop(name)
        else:
            value = definition.default_binary

        if definition.required and not value:
            err = exc.RequiredInputError(target='job', field=definition.name, internal_name=definition.internal_name)
            errors.append(err)
            continue

        if definition.allowed_binaries and value:
            values = value
            if isinstance(value, str):
                values = [value]

            if isinstance(values, list):
                values = [json.dumps(v).encode() if v else v for v in values]

            if set(values) - set(definition.allowed_binaries) or definition.type != 'text-multiple' and len(values) > 1:
                err = exc.InvalidEnumError(
                    target='job',
                    field=definition.name,
                    allowed_values=definition.allowed_values,
                    internal_name=definition.internal_name
                )
                errors.append(err)
                continue

        input_value_mapping[definition] = value

    for name in list(input_values):
        if name in file_mapping and name not in input_files:
            input_files[file_mapping[name]] = input_values.pop(name)

    for name, definition in file_map_definitions.items():
        if name in input_files:
            value = input_files.pop(name)
        else:
            value = None

        if definition.required and not value:
            err = exc.RequiredInputError(target='job', field=definition.name, internal_name=definition.internal_name)
            errors.append(err)
            continue

        input_file_mapping[definition] = value

    unexpected_inputs = set(input_values) | set(input_files)
    if unexpected_inputs:
        for input_ in unexpected_inputs:
            err = exc.UnknownInputError(target='job', field=input_)
            errors.append(err)

    if errors:
        raise exc.InputErrorGroup(*errors)

    return input_value_mapping, input_file_mapping


def create_job(form_id, name, input_values, input_files, execute, parent_execution=None):
    form = services.form.form_by_id(form_id=form_id)
    policy.context.authorize(form, action='view')

    errors = []
    if not name:
        err = exc.RequiredInputError(target='job', field='name')
        errors.append(err)
    else:
        if model.job.Job.query().filter_by(name=name).first():
            err = exc.UniqueInputConstraintError(target='job', field='name', value=name)
            errors.append(err)

    try:
        values_mapping, files_mapping = verify_inputs(
            input_value_definitions=form.input_value_definitions,
            input_file_definitions=form.input_file_definitions,
            input_values=input_values,
            input_files=input_files
        )
    except exc.InputErrorGroup as e:
        e.extend(errors)
        raise
    else:
        if errors:
            raise exc.InputErrorGroup(*errors)

    input_value_instances = []
    input_file_instances = []

    values_mapping = {
        k: (json.dumps(v).encode() if v and not isinstance(v, bytes) else v)
        for k, v in values_mapping.items()
    }

    for definition, value in values_mapping.items():
        input_value_instance = model.interface.InputValueInstance(
            name=definition.name,
            internal_name=definition.internal_name,
            raw_value=value
        )
        input_value_instances.append(input_value_instance)

    for definition, value in files_mapping.items():
        filename = definition.internal_name
        if not hasattr(value, 'read'):
            value = InputFileProxy.from_object(obj=value, filename=filename)

        binary = value.read()
        if isinstance(binary, str):
            binary = binary.encode()
        if hasattr(value, 'filename'):
            filename = value.filename

        input_file_instance = model.interface.InputFileInstance(
            name=definition.name,
            internal_name=definition.internal_name,
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
    if execute:
        create_job_execution(job, parent_execution=parent_execution)

    model.conn.add(job)

    return job


@policy.context.post_authorize(action='view')
def job_by_id(job_id):
    return model.job.Job.query().filter_by(id=job_id).one()


@policy.context.post_filter(action='view')
def job_executions_by_id(job_id):
    return model.job.JobExecution.query().filter_by(job_id=job_id).all()


def create_job_execution_by_job_id(job_id):
    job = job_by_id(job_id=job_id)
    return create_job_execution(job=job)


def create_job_execution(job, parent_execution=None):
    execution = model.job.JobExecution(job=job, creator=policy.context.subject, parent_execution=parent_execution)

    policy.context.authorize(execution, action='create')
    model.conn.add(execution)

    return execution


def input_file_instance_by_id(input_file_instance_id) -> model.interface.InputFileInstance:
    return model.interface.InputFileInstance.query().filter_by(id=input_file_instance_id).one()


def delete_job_by_id(job_id):
    delete_job(job=job_by_id(job_id=job_id))


@policy.context.post_authorize(action='view')
def delete_job(job):
    model.conn.delete(job)

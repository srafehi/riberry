from typing import List, Dict

from sqlalchemy import desc

from riberry import model, policy
from riberry import services


@policy.context.post_filter(action='view')
def all_forms() -> List[model.interface.Form]:
    return model.interface.Form.query().all()


@policy.context.post_authorize(action='view')
def form_by_id(form_id) -> model.interface.Form:
    return model.interface.Form.query().filter_by(id=form_id).one()


@policy.context.post_authorize(action='view')
def form_by_internal_name(internal_name) -> model.interface.Form:
    return model.interface.Form.query().filter_by(
        internal_name=internal_name,
    ).one()


@policy.context.post_filter(action='view')
def forms_by_application_id(application_id):
    application = services.application.application_by_id(application_id=application_id)
    return application.forms


def create_form(application, instance, name, internal_name, version, description, input_files, input_values) -> model.interface.Form:
    form = model.interface.Form(
        application=application,
        instance=instance,
        name=name,
        internal_name=internal_name,
        version=version,
        description=description,
        input_file_definitions=[model.interface.InputFileDefinition(**d) for d in input_files],
        input_value_definitions=[model.interface.InputValueDefinition(**d) for d in input_values]
    )

    policy.context.authorize(form, action='create')
    model.conn.add(form)
    return form


def update_form(form: model.interface.Form, attributes: Dict):
    for attr in {'name', 'version', 'description'} & set(attributes):
        setattr(form, attr, attributes[attr])
    return form


@policy.context.post_authorize(action='view')
def file_definition_by_internal_name(form, internal_name) -> model.interface.InputFileDefinition:
    return model.interface.InputFileDefinition.query().filter_by(
        form=form,
        internal_name=internal_name,
    ).one()


@policy.context.post_authorize(action='view')
def value_definition_by_internal_name(form, internal_name) -> model.interface.InputValueDefinition:
    return model.interface.InputValueDefinition.query().filter_by(
        form=form,
        internal_name=internal_name,
    ).one()


def update_file_definition(definition: model.interface.InputFileDefinition, attributes: Dict):
    for attr in {'required', 'type', 'name', 'description', 'accept'} & set(attributes):
        setattr(definition, attr, attributes[attr])

    return definition


def update_value_definition(definition: model.interface.InputValueDefinition, attributes: Dict):
    for attr in {'required', 'type', 'name', 'description', 'default_binary'} & set(attributes):
        setattr(definition, attr, attributes[attr])

    if 'allowed_binaries' in attributes:
        current = set(definition.allowed_binaries)
        present = set(attributes['allowed_binaries'])
        removed = current - present
        if current != present:
            definition.allowed_binaries = [enum for enum in attributes['allowed_binaries'] if enum not in removed]
    elif definition.allowed_binaries:
        definition.allowed_binaries = []

    return definition


def job_executions_by_id(form_id, limit=50):
    form = form_by_id(form_id=form_id)
    return model.job.JobExecution.query().filter(
        (model.job.JobExecution.job_id == model.job.Job.id) &
        (model.job.Job.form_id == form.id)
    ).order_by(desc(model.job.JobExecution.updated)).limit(limit).all()

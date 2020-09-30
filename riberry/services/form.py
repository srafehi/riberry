from typing import List, Dict

from sqlalchemy import desc

from riberry import model, policy
from riberry import services


def all_forms() -> List[model.interface.Form]:
    return model.interface.Form.query().all()


def form_by_id(form_id) -> model.interface.Form:
    return model.interface.Form.query().filter_by(id=form_id).one()


def form_by_internal_name(internal_name) -> model.interface.Form:
    return model.interface.Form.query().filter_by(
        internal_name=internal_name,
    ).one()


def forms_by_application_id(application_id):
    application = services.application.application_by_id(application_id=application_id)
    return application.forms


def create_form(application, instance, name, internal_name, version, description) -> model.interface.Form:
    form = model.interface.Form(
        application=application,
        instance=instance,
        name=name,
        internal_name=internal_name,
        version=version,
        description=description,
    )

    model.conn.add(form)
    return form


def update_form(form: model.interface.Form, attributes: Dict):
    for attr in {'name', 'version', 'description'} & set(attributes):
        setattr(form, attr, attributes[attr])
    return form


def input_definition_by_internal_name(
        form: model.interface.Form,
        internal_name: str
) -> model.interface.InputDefinition:
    return model.interface.InputDefinition.query().filter_by(
        form=form,
        internal_name=internal_name,
    ).one()


def update_input_definition(
        definition: model.interface.InputDefinition,
        attributes: Dict,
) -> model.interface.InputDefinition:
    for attr in {'type', 'name', 'description', 'sequence', 'definition'} & set(attributes):
        setattr(definition, attr, attributes[attr])

    return definition


def job_executions_by_id(form_id, limit=50):
    form = form_by_id(form_id=form_id)
    return model.job.JobExecution.query().filter(
        (model.job.JobExecution.job_id == model.job.Job.id) &
        (model.job.Job.form_id == form.id)
    ).order_by(desc(model.job.JobExecution.updated)).limit(limit).all()

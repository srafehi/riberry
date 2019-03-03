from typing import List

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
def form_by_interface_and_instance_names(interface_name, interface_version, instance_name) -> model.interface.Form:
    interface: model.interface.ApplicationInterface = model.interface.ApplicationInterface.query().filter_by(
        internal_name=interface_name,
        version=interface_version,
    ).one()

    instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
        internal_name=instance_name,
    ).one()

    return model.interface.Form.query().filter_by(
        instance=instance,
        interface=interface,
    ).one()


def create_form(instance, interface) -> model.interface.Form:
    assert instance.application.id == interface.application.id, \
        'Instance and interface do not belong to the same application'

    form = model.interface.Form(
        instance=instance,
        interface=interface
    )

    policy.context.authorize(form, action='create')
    model.conn.add(form)
    return form


def job_executions_by_id(form_id, limit=50):
    form = form_by_id(form_id=form_id)
    return model.job.JobExecution.query().filter(
        (model.job.JobExecution.job_id == model.job.Job.id) &
        (model.job.Job.form_id == form.id)
    ).order_by(desc(model.job.JobExecution.updated)).limit(limit).all()
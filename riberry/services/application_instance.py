from typing import List

from riberry import model, policy, services


@policy.context.post_filter(action='view')
def all_application_instances() -> List[model.application.ApplicationInstance]:
    return model.application.ApplicationInstance.query().all()


@policy.context.post_authorize(action='view')
def application_instance_by_id(application_instance_id) -> model.application.ApplicationInstance:
    return model.application.ApplicationInstance.query().filter_by(id=application_instance_id).one()


@policy.context.post_filter(action='view')
def instances_by_application_id(application_id) -> List[model.application.ApplicationInstance]:
    application = services.application.application_by_id(application_id=application_id)
    return application.instances


def create_application_instance(application_id, name, internal_name) -> model.application.ApplicationInstance:
    application = services.application.application_by_id(application_id=application_id)
    application_instance = model.application.ApplicationInstance(
        application=application,
        name=name,
        internal_name=internal_name
    )

    policy.context.authorize(application_instance, action='create')
    model.conn.add(application_instance)
    model.conn.commit()
    return application_instance

from typing import List

from riberry import model, policy, services


@policy.context.post_filter(action='view')
def all_application_instances() -> List[model.application.ApplicationInstance]:
    return model.application.ApplicationInstance.query().all()


@policy.context.post_authorize(action='view')
def application_instance_by_id(application_instance_id) -> model.application.ApplicationInstance:
    return model.application.ApplicationInstance.query().filter_by(id=application_instance_id).one()


@policy.context.post_filter(action='view')
def instances_by_application_id(application_id):
    application = services.application.application_by_id(application_id=application_id)
    return application.instances

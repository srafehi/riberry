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

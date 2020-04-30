"""
Defines the query authorizer which joins up to the allowed
Applications via the following dependency tree:

* Application
  * ApplicationInstance
    * Heartbeat
    * ApplicationInstanceSchedule

"""

from sqlalchemy.orm import Query

import riberry
from .base import StepResult, PermissionDomainQueryAuthorizer

application_authorizer = PermissionDomainQueryAuthorizer()

application_authorizer.register_chain(
    riberry.model.application.ApplicationInstanceSchedule,
    riberry.model.application.ApplicationInstance,
)

application_authorizer.register_chain(
    riberry.model.application.Heartbeat,
    riberry.model.application.ApplicationInstance,
)

application_authorizer.register_chain(
    riberry.model.application.ApplicationInstance,
    riberry.model.application.Application,
)


@application_authorizer.register_resolver(riberry.model.application.Application)
def application_filter(query: Query, context):
    app_cls = riberry.model.application.Application
    return StepResult(
        query,
        None,
        app_cls.id.in_(context.permissions.get(context.requested_permission, [])),
    )

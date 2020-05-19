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
from .base import StepResult, PermissionDomainQueryAuthorizer, Node

application_authorizer = PermissionDomainQueryAuthorizer()

_node_tree = Node(riberry.model.application.Application, (
    Node(riberry.model.application.ApplicationInstance, (
        Node(riberry.model.application.Heartbeat),
        Node(riberry.model.application.ApplicationInstanceSchedule),
    )),
))

application_authorizer.register_node(node=_node_tree)


@application_authorizer.register_resolver(riberry.model.application.Application)
def application_filter(query: Query, context):
    app_cls = riberry.model.application.Application
    return StepResult(
        query,
        None,
        app_cls.id.in_(context.permissions.get(context.requested_permission, [])),
    )

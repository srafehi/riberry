from riberry import policy
from . import application, application_instance, application_interface, instance_interface


def fetch_relationship(model_object, attribute, action):
    resource = getattr(model_object, attribute)

    if isinstance(resource, list):
        return policy.context.filter(resources=resource, action=action)
    return policy.context.authorize(resource=resource, action=action)
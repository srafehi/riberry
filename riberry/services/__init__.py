from riberry import policy
from . import application, application_instance, application_interface, instance_interface, auth, job


def fetch_relationship(model_object, attribute, action):
    resource = getattr(model_object, attribute)

    if isinstance(resource, list):
        return policy.context.filter(resources=resource, action=action)
    if policy.context.authorize(resource=resource, action=action, on_deny=None) is not False:
        return resource
    return None

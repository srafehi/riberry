from riberry import policy as rib_policy
from . import application, application_instance, form, auth, job, job_executions, self, policy


def fetch_relationship(model_object, attribute, action):
    resource = getattr(model_object, attribute)

    if isinstance(resource, list):
        return rib_policy.context.filter(resources=resource, action=action)
    if rib_policy.context.authorize(resource=resource, action=action, on_deny=None) is not False:
        return resource
    return None

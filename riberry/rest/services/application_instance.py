from typing import List, Dict
from riberry import services
from riberry.rest import view_models


def all_application_instances(options) -> List[Dict]:
    application_instances = services.application_instance.all_application_instances()
    return [view_models.ApplicationInstance(model=app, options=options).to_dict() for app in application_instances]


def application_instance_by_id(application_instance_id, options) -> Dict:
    application_instance = services.application_instance.\
        application_instance_by_id(application_instance_id=application_instance_id)
    return view_models.ApplicationInstance(model=application_instance, options=options).to_dict()


def instances_by_application_id(application_id, options) -> List[Dict]:
    instances = services.application_instance.instances_by_application_id(application_id=application_id)
    return [view_models.ApplicationInstance(model=instance, options=options).to_dict() for instance in instances]


def create_application_instance(application_id, name, internal_name) -> Dict:
    application_instance = services.application_instance.create_application_instance(
        application_id=application_id,
        name=name,
        internal_name=internal_name,
    )
    return view_models.ApplicationInstance(application_instance, options=None).to_dict()

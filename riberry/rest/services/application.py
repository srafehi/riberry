from typing import List, Dict

from riberry import services
from riberry.rest import view_models


def all_applications(options) -> List[Dict]:
    applications = services.application.all_applications()
    return [view_models.Application(model=app, options=options).to_dict() for app in applications]


def application_by_id(application_id, options) -> Dict:
    application = services.application.application_by_id(application_id=application_id)
    return view_models.Application(application, options).to_dict()


def create_application(name, internal_name, description, type) -> Dict:
    application = services.application.create_application(
        name=name,
        internal_name=internal_name,
        description=description,
        type=type
    )
    return view_models.Application(application, options=None).to_dict()

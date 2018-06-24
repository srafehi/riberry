from typing import List, Dict

from riberry import model, policy, services


@policy.context.post_filter(action='view')
def all_application_instances() -> List[model.application.ApplicationInstance]:
    return model.application.ApplicationInstance.query().all()


@policy.context.post_authorize(action='view')
def application_instance_by_id(application_instance_id) -> model.application.ApplicationInstance:
    return model.application.ApplicationInstance.query().filter_by(id=application_instance_id).one()


@policy.context.post_authorize(action='view')
def application_instance_by_internal_name(internal_name) -> model.application.ApplicationInstance:
    return model.application.ApplicationInstance.query().filter_by(internal_name=internal_name).one()


@policy.context.post_filter(action='view')
def instances_by_application_id(application_id) -> List[model.application.ApplicationInstance]:
    application = services.application.application_by_id(application_id=application_id)
    return application.instances


def create_application_instance(application, name, internal_name, schedules: List[Dict]) -> model.application.ApplicationInstance:
    application_instance = model.application.ApplicationInstance(
        application=application,
        name=name,
        internal_name=internal_name,
        schedules=create_application_instance_schedules(attributes_dict=schedules),
    )

    policy.context.authorize(application_instance, action='create')
    model.conn.add(application_instance)
    return application_instance


def create_application_instance_schedules(attributes_dict):
    return [
        model.application.ApplicationInstanceSchedule(
            days=schedule['days'],
            start_time=schedule['start_time'],
            end_time=schedule['end_time'],
            timezone=schedule['timezone'],
            parameter=schedule['parameter'],
            value=schedule['value'],
            priority=schedule['priority'],
        ) for schedule in attributes_dict
    ]


def update_application_instance(application_instance: model.application.ApplicationInstance, attributes: Dict):
    for attr in {'name'} & set(attributes):
        setattr(application_instance, attr, attributes[attr])
    return application_instance

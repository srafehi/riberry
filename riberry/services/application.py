from typing import List

from riberry import model, policy


@policy.context.post_filter(action='view')
def all_applications() -> List[model.application.Application]:
    return model.application.Application.query().all()


@policy.context.post_authorize(action='view')
def application_by_id(application_id) -> model.application.Application:
    return model.application.Application.query().filter_by(id=application_id).one()


@policy.context.post_authorize(action='view')
def application_by_internal_name(internal_name) -> model.application.Application:
    return model.application.Application.query().filter_by(internal_name=internal_name).one()


def create_application(name, internal_name, description, type):
    app = model.application.Application(
        name=name,
        internal_name=internal_name,
        description=description,
        type=type
    )

    policy.context.authorize(app, action='create')
    model.conn.add(app)
    model.conn.commit()

    return app


def update_application_by_id(application_id, attributes):
    return update_application(application_by_id(application_id), attributes)


def update_application_by_internal_name(internal_name, attributes):
    return update_application(application_by_internal_name(internal_name), attributes)


def update_application(app, attributes):
    for attr in {'name', 'description', 'type'} & set(attributes):
        setattr(app, attr, attributes[attr])

    return app

from typing import List

from riberry import model, policy


@policy.context.post_filter(action='view')
def all_applications() -> List[model.application.Application]:
    return model.application.Application.query().all()


@policy.context.post_authorize(action='view')
def application_by_id(application_id) -> model.application.Application:
    return model.application.Application.query().filter_by(id=application_id).one()


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
from typing import List

from riberry import model, policy
from riberry import services


@policy.context.post_filter(action='view')
def all_forms() -> List[model.interface.Form]:
    return model.interface.Form.query().all()


@policy.context.post_authorize(action='view')
def form_by_id(form_id) -> model.interface.Form:
    return model.interface.Form.query().filter_by(id=form_id).one()


def create_form(instance, interface) -> model.interface.Form:
    assert instance.application.id == interface.application.id, \
        'Instance and interface do not belong to the same application'

    form = model.interface.Form(
        instance=instance,
        interface=interface
    )

    policy.context.authorize(form, action='create')
    model.conn.add(form)
    return form

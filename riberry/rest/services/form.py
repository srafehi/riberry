from typing import List, Dict

from riberry import services
from riberry.rest import view_models


def all_forms(options) -> List[Dict]:
    forms = services.form.all_forms()
    return [view_models.Form(model=app, options=options).to_dict() for app in forms]


def form_by_id(form_id, options) -> Dict:
    form = services.form.form_by_id(form_id)
    return view_models.Form(model=form, options=options).to_dict()


def create_form(instance_id, interface_id, groups) -> Dict:
    form = services.form.create_form(instance_id, interface_id, groups)
    return view_models.Form(model=form, options=None).to_dict()

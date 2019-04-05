import os

import riberry
from .base import RiberryApplication
from .context import Context
from .util.misc import Proxy

__cache = dict(
    app_name={},
)
__cache_app_name = __cache['app_name']


def get_instance_name(raise_on_none=True) -> str:
    if 'RIBERRY_INSTANCE' not in os.environ and raise_on_none:
        raise EnvironmentError("Environment variable 'RIBERRY_INSTANCE' not set")
    return os.environ.get('RIBERRY_INSTANCE')


def get_application_name() -> str:
    instance_name = get_instance_name(raise_on_none=True)
    if instance_name not in __cache_app_name:
        __cache_app_name[instance_name] = get_instance_model().application.internal_name
    return __cache_app_name[instance_name]


def get_instance_model() -> riberry.model.application.ApplicationInstance:
    return riberry.model.application.ApplicationInstance.query().filter_by(
        internal_name=get_instance_name(raise_on_none=True),
    ).one()


def is_current_instance(instance_name: str) -> bool:
    return bool(instance_name and get_instance_name(raise_on_none=False) == instance_name)


current_riberry_app: RiberryApplication = Proxy(
    getter=lambda: RiberryApplication.by_name(name=get_application_name())
)
current_context: Context = Proxy(
    getter=lambda: current_riberry_app.context
)

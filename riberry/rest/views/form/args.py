from webargs import fields as w
from marshmallow import fields as m
from riberry.rest.views.base import base_args


base = {
    'bla': m.Int(),
    **base_args
}
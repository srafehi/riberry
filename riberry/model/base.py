import pendulum
from sqlalchemy import Column, Sequence, Integer
from sqlalchemy.ext.declarative import declarative_base

from riberry import model


class _BaseMixin:

    @classmethod
    def query(cls):
        return model.conn.query(cls)

    def __repr__(self):
        attribute_names = (['id'] if hasattr(self, 'id') else []) + getattr(self, '__reprattrs__', [])
        attributes = ', '.join(f'{attr}={repr(getattr(self, attr))}' for attr in attribute_names)
        return f'<{type(self).__name__} {attributes}>'


class _IdBuilder:

    def __init__(self, id_type):
        self.type = id_type

    def build(self, sequence='SEQUENCE_PK'):
        return Column(self.type, Sequence(sequence), primary_key=True)


Base = declarative_base(cls=_BaseMixin)
id_builder = _IdBuilder(id_type=Integer)


def utc_now():
    return pendulum.DateTime.utcnow()

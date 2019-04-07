import pendulum
from sqlalchemy import Column, Sequence, Integer, MetaData
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


meta = MetaData(
    naming_convention={
        'ix': 'ix_%(column_0_label)s',
        'uq': 'uq_%(table_name)s_%(column_0_N_name)s',
        'ck': 'ck_%(table_name)s_%(constraint_name)s',
        'fk': 'fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s',
        'pk': 'pk_%(table_name)s'
    }
)

Base = declarative_base(cls=_BaseMixin, metadata=meta)
id_builder = _IdBuilder(id_type=Integer)


def utc_now():
    return pendulum.DateTime.utcnow()

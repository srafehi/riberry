from typing import Union

import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.pool

from . import misc, application, group, auth, interface, job, base
from ..util.misc import import_from_string

ScopedSessionExt = Union[sqlalchemy.orm.Session, sqlalchemy.orm.scoping.ScopedSession]


class __ModelProxy:
    raw_session: ScopedSessionExt = None
    raw_engine = None

    def __getattr__(self, item):
        return getattr(self.raw_session, item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.raw_session.rollback()
        self.raw_session.remove()


# noinspection PyTypeChecker
conn: ScopedSessionExt = __ModelProxy()


def init(url='sqlite://', engine_settings=None, connection_arguments=None):
    engine_defaults = dict(
        poolclass='sqlalchemy.pool:QueuePool',
        pool_use_lifo=True,
        pool_pre_ping=True,
        pool_recycle=360,
    )
    engine_settings = {**engine_defaults, **(engine_settings or {})}
    engine_settings['poolclass'] = import_from_string(engine_settings.pop('poolclass') or 'sqlalchemy.pool:QueuePool')
    connection_arguments = connection_arguments or {}

    __ModelProxy.raw_engine = sqlalchemy.create_engine(
        url,
        **engine_settings,
        connect_args=connection_arguments,
    )
    __ModelProxy.raw_session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=__ModelProxy.raw_engine))
    base.Base.metadata.create_all(__ModelProxy.raw_engine)

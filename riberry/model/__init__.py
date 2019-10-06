from typing import Union

import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.pool

from . import misc, application, group, auth, interface, job, base

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


def init(url='sqlite://', **config):
    __ModelProxy.raw_engine = sqlalchemy.create_engine(
        url,
        echo=config.get('echo', False),
        poolclass=sqlalchemy.pool.QueuePool,
        pool_use_lifo=True,
        pool_pre_ping=True,
        pool_recycle=360,
        **config.get('engine_settings', {}),
        connect_args=config.get('connection_arguments', {})
    )
    __ModelProxy.raw_session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=__ModelProxy.raw_engine))
    base.Base.metadata.create_all(__ModelProxy.raw_engine)

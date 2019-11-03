import os
from typing import Union, Any
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.pool
import sqlalchemy.engine

from . import misc, application, group, auth, interface, job, base
from ..util.misc import import_from_string
from .. import log

log = log.make(__name__)

ScopedSessionExt = Union[sqlalchemy.orm.Session, sqlalchemy.orm.scoping.ScopedSession]


class __ModelProxy:
    raw_session: ScopedSessionExt = None
    raw_engine: sqlalchemy.engine.Engine = None
    _pid_created_in: int = os.getpid()

    def _check_pid(self):
        """ Disposes SQLAlchemy engine if current process is a fork. """

        current_pid = os.getpid()
        if self._pid_created_in != current_pid:
            log.debug(f'riberry.model.conn:: Process forked from pid {self._pid_created_in}, disposing of sqla engine')
            self._pid_created_in = current_pid
            self.dispose_engine()

    def __getattr__(self, item: str) -> Any:
        self._check_pid()
        return getattr(self.raw_session, item)

    def __enter__(self) -> '__ModelProxy':
        self._check_pid()
        self.raw_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.raw_session.rollback()
        self.dispose_session()

    def dispose_engine(self):
        """ Disposes of the current SQLAlchemy engine. """
        self.dispose_session()
        try:
            self.raw_engine.dispose()
        except:
            log.exception('riberry.model.conn:: Encountered an error while disposing current sqla engine')

    def dispose_session(self):
        """ Disposes of the current SQLAlchemy session. """
        try:
            self.raw_session.remove()
        except:
            log.exception('riberry.model.conn:: Encountered an error while removing current sqla session')


conn: Union[ScopedSessionExt, __ModelProxy] = __ModelProxy()


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

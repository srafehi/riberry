import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.pool

from . import misc, application, group, auth, interface, job, base


class __ModelProxy:
    raw_session = None
    raw_engine = None

    def __getattr__(self, item):
        return getattr(self.raw_session, item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove()


conn: sqlalchemy.orm.session.Session = __ModelProxy()


def init(url='sqlite://', **config):
    __ModelProxy.raw_engine = sqlalchemy.create_engine(
        url,
        echo=config.get('echo', False),
        poolclass=sqlalchemy.pool.QueuePool,
        pool_use_lifo=True,
        pool_pre_ping=True,
        connect_args=config.get('connection_arguments', {})
    )
    __ModelProxy.raw_session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=__ModelProxy.raw_engine))
    base.Base.metadata.create_all(__ModelProxy.raw_engine)

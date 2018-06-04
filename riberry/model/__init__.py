from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, session

from . import application, group, auth, interface, job, misc, base


class __ModelProxy:
    raw_session = None
    raw_engine = None

    def __getattr__(self, item):
        return getattr(self.raw_session, item)


conn: session.Session = __ModelProxy()


def init(url='sqlite://', **config):
    __ModelProxy.raw_engine = create_engine(
        url,
        echo=config.get('echo', False),
        connect_args=config.get('connection_arguments', {})
    )
    __ModelProxy.raw_session = scoped_session(sessionmaker(bind=__ModelProxy.raw_engine))
    base.Base.metadata.create_all(__ModelProxy.raw_engine)
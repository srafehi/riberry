from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, session

from . import admin, auth, execution, job, base


class __ModelProxy:
    raw_session = None
    raw_engine = None
    
    def __getattr__(self, item):
        return getattr(self.raw_session, item)


conn: session.Session = __ModelProxy()


def init(**config):
    __ModelProxy.raw_engine = create_engine(config['url'], echo=config.get('echo', False))
    __ModelProxy.raw_session = scoped_session(sessionmaker(bind=__ModelProxy.raw_engine))
    base.Base.metadata.create_all(__ModelProxy.raw_engine)

from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship

from ..base import Base, id_builder
from ..config import config


class User(Base):
    __tablename__ = 'users'
    __reprattrs__ = ['username']

    id = id_builder.build()
    username = Column('username', String(32), unique=True)
    password = Column('password', String(32), unique=True)
    details: 'UserDetails' = relationship('UserDetails', uselist=False, back_populates='user')

    @classmethod
    def authenticate(cls, username, password):
        if not config.authentication_provider.authenticate(username=username, password=password):
            raise Exception('Unauthorized')
        return cls.query().filter_by(username=username).first()


class UserDetails(Base):
    __tablename__ = 'user_details'

    id = id_builder.build()
    user_id = Column(id_builder.type, ForeignKey('users.id'))
    user = relationship('User', back_populates='details')

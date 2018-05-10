from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship, validates

from ..base import Base, id_builder
from ..config import config


class User(Base):
    __tablename__ = 'users'
    __reprattrs__ = ['username']

    id = id_builder.build()
    username = Column('username', String(32), nullable=False, unique=True)
    password = Column('password', String(128))
    details: 'UserDetails' = relationship('UserDetails', uselist=False, back_populates='user')

    @classmethod
    def authenticate(cls, username, password):
        if not config.authentication_provider.authenticate(username=username, password=password):
            raise Exception('Unauthorized')
        return cls.query().filter_by(username=username).first()

    @validates('username')
    def validate_username(self, key, username):
        if not username or len(username) < 3:
            raise ValueError('Usernames must be 3+ characters long')
        return username


class UserDetails(Base):
    __tablename__ = 'user_details'

    id = id_builder.build()
    user_id = Column(id_builder.type, ForeignKey('users.id'))
    user = relationship('User', back_populates='details')

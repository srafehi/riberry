import pendulum
import datetime
from sqlalchemy import Column, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship, validates

from ..base import Base, id_builder
from ..config import config
import re


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
    def validate_username(self, _, username):
        if not username or len(username) < 3:
            raise ValueError(f'User.username :: usernames must be 3+ characters long. Received {repr(username)}')
        return username


class UserDetails(Base):
    __tablename__ = 'user_details'

    id = id_builder.build()
    user_id = Column(id_builder.type, ForeignKey('users.id'), nullable=False)
    user = relationship('User', back_populates='details')

    first_name = Column(String(32), nullable=False)
    last_name = Column(String(32), nullable=False)
    department = Column(String(64))
    email = Column(String(128))
    mobile = Column(String(24))
    updated: datetime = Column(DateTime, default=lambda: pendulum.now(tz='utc'))

    @property
    def full_name(self):
        return f'{self.first_name} {self.last_name}'

    @validates('email')
    def validate_email(self, _, email):
        if not email or not re.match(r'[^@]+@[^@]+\.[^@]+', email or ''):
            raise ValueError(f'UserDetails.email :: Invalid email received ({repr(email)})')
        return email

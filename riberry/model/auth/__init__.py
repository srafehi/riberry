import datetime
import re
from typing import AnyStr, Dict, List

import jwt
import pendulum
from sqlalchemy import Column, String, ForeignKey, DateTime
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, validates

from riberry import model
from riberry.model import base
from riberry.model.config import config


class User(base.Base):
    __tablename__ = 'users'
    __reprattrs__ = ['username']

    id = base.id_builder.build()
    username = Column('username', String(32), nullable=False, unique=True)
    password = Column('password', String(128))
    details: 'UserDetails' = relationship('UserDetails', uselist=False, back_populates='user')

    # associations
    group_associations: List['model.group.ResourceGroupAssociation'] = model.group.ResourceGroupAssociation.make_relationship(
        resource_id=id,
        resource_type=model.group.ResourceType.user
    )

    # proxies
    groups: List['model.group.Group'] = association_proxy('group_associations', 'group')

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


class UserDetails(base.Base):
    __tablename__ = 'user_details'

    id = base.id_builder.build()
    user_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    user = relationship('User', back_populates='details')

    first_name = Column(String(32), nullable=False)
    last_name = Column(String(32), nullable=False)
    department = Column(String(64))
    email = Column(String(128))
    mobile = Column(String(24))
    updated: datetime = Column(DateTime, default=base.utc_now)

    @property
    def full_name(self):
        return f'{self.first_name} {self.last_name}'

    @validates('email')
    def validate_email(self, _, email):
        if not email or not re.match(r'[^@]+@[^@]+\.[^@]+', email or ''):
            raise ValueError(f'UserDetails.email :: Invalid email received ({repr(email)})')
        return email


class AuthToken:

    @staticmethod
    def create(user: User, expiry_delta: datetime.timedelta=datetime.timedelta(hours=24)) -> AnyStr:
        iat: pendulum.DateTime = base.utc_now()
        exp: pendulum.DateTime = iat + expiry_delta

        return jwt.encode({
            'iat': iat.int_timestamp,
            'exp': exp.int_timestamp,
            'name': user.username,
            'group': None
        }, config.secrets['jwt_secret'], algorithm='HS256')

    @staticmethod
    def verify(token: AnyStr) -> Dict:
        return jwt.decode(token, config.secrets['jwt_secret'], algorithms=['HS256'])


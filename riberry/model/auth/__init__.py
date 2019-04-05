import datetime
import re
from typing import AnyStr, Dict, List

import jwt
import pendulum
from sqlalchemy import Column, String, ForeignKey, DateTime, desc
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, validates, joinedload

from riberry import model, exc
from riberry.model import base
from riberry.config import config


class User(base.Base):
    __tablename__ = 'users'
    __reprattrs__ = ['username']

    id = base.id_builder.build()
    username = Column(String(48), nullable=False, unique=True)
    password = Column(String(512))
    auth_provider = Column(String(32), nullable=False, default=config.authentication.providers.default)
    details: 'UserDetails' = relationship('UserDetails', uselist=False, back_populates='user')

    # associations
    group_associations: List['model.group.ResourceGroupAssociation'] = model.group.ResourceGroupAssociation.make_relationship(
        resource_id=id,
        resource_type=model.misc.ResourceType.user
    )
    jobs: List['model.job.Job'] = relationship(
        'Job',
        order_by=lambda: desc(model.job.Job.created),
        back_populates='creator')
    executions: List['model.job.JobExecution'] = relationship(
        'JobExecution',
        order_by=lambda: desc(model.job.JobExecution.updated),
        back_populates='creator')
    notifications: List['model.misc.UserNotification'] = relationship(
        'UserNotification',
        order_by=lambda: desc(model.misc.UserNotification.created),
        back_populates='user')

    # proxies
    groups: List['model.group.Group'] = association_proxy('group_associations', 'group')

    @property
    def forms(self) -> List['model.interface.Form']:
        return model.interface.Form.query().filter(
            (model.group.ResourceGroupAssociation.group_id.in_(o.group_id for o in self.group_associations)) &
            (model.group.ResourceGroupAssociation.resource_type == model.misc.ResourceType.form) &
            (model.interface.Form.id == model.group.ResourceGroupAssociation.resource_id)
        ).all()

    @property
    def applications(self) -> List['model.application.Application']:
        forms = model.interface.Form.query().filter(
            model.interface.Form.id.in_(form.id for form in self.forms)
        ).options(
            joinedload(model.interface.Form.application)
        ).all()

        return [form.application for form in forms]

    @classmethod
    def authenticate(cls, username, password):
        existing_user: cls = cls.query().filter_by(username=username).first()
        if existing_user:
            provider = config.authentication[existing_user.auth_provider]
        else:
            provider = config.authentication.default_provider

        if not provider.authenticate(username=username, password=password):
            raise exc.AuthenticationError

        return cls.query().filter_by(username=username).first()

    @validates('username')
    def validate_username(self, _, username):
        if not username or len(username) < 3:
            raise ValueError(f'User.username :: usernames must be 3+ characters long. Received {repr(username)}')
        return username

    @classmethod
    def secure_password(cls, password: str, provider_name=None) -> bytes:
        provider = config.authentication[provider_name or config.authentication.providers.default]
        password = provider.secure_password(password=password)
        return password


class UserDetails(base.Base):
    __tablename__ = 'user_details'

    id = base.id_builder.build()
    user_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    user: 'User' = relationship('User', back_populates='details')

    first_name = Column(String(64))
    last_name = Column(String(64))
    display_name = Column(String(128))
    department = Column(String(128))
    email = Column(String(128))
    updated: datetime = Column(DateTime(timezone=True), default=base.utc_now)

    @property
    def full_name(self):
        if self.first_name and self.last_name:
            return f'{self.first_name} {self.last_name}'
        else:
            return self.first_name or self.last_name or self.display_name or self.user.username

    @validates('email')
    def validate_email(self, _, email):
        if email and not re.match(r'[^@]+@[^@]+\.[^@]+', email or ''):
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
            'subject': user.username
        }, config.authentication.token.secret, algorithm='HS256')

    @staticmethod
    def verify(token: AnyStr) -> Dict:
        try:
            return jwt.decode(token, config.authentication.token.secret, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            raise exc.SessionExpired
        except Exception:
            raise exc.AuthenticationError


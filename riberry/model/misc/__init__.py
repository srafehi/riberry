import enum
from datetime import datetime
from typing import List

from sqlalchemy import Binary, String, Column, Float, ForeignKey, Boolean, DateTime, Index, Enum
from sqlalchemy.orm import relationship

from riberry import model
from riberry.model import base


class Document(base.Base):
    __tablename__ = 'document'

    id = base.id_builder.build()
    type = Column(String(24), nullable=False, default='markdown')
    content = Column(Binary, nullable=False)


class Event(base.Base):
    __tablename__ = 'event'
    __reprattrs__ = ['name', 'root_id']

    # columns
    id = base.id_builder.build()
    name: str = Column(String(64), nullable=False)
    time: float = Column(Float, nullable=False)
    root_id: str = Column(String(36), nullable=False)
    task_id: str = Column(String(36), nullable=False)
    data: str = Column(String(1024))
    binary: bytes = Column(Binary)


class NotificationType(enum.Enum):
    info = 'info'
    warning = 'warning'
    success = 'success'
    error = 'error'
    alert = 'alert'


class UserNotification(base.Base):
    __tablename__ = 'notification_user'
    __table_args__ = (
        Index('u_n__idx_job_id', 'user_id', 'read'),
    )

    # columns
    id = base.id_builder.build()
    user_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    notification_id = Column(base.id_builder.type, ForeignKey('notification.id'), nullable=False)
    created: datetime = Column(DateTime, default=base.utc_now)
    read = Column(Boolean, nullable=False, default=False)

    # associations
    user: 'model.auth.User' = relationship('User', back_populates='notifications')
    notification: 'Notification' = relationship('Notification', back_populates='user_notifications')


class Notification(base.Base):
    __tablename__ = 'notification'

    # columns
    id = base.id_builder.build()
    type = Column(Enum(NotificationType), nullable=False, default=NotificationType.info)
    message = Column(String(128), nullable=False)

    # associations
    user_notifications: List['UserNotification'] = relationship('UserNotification', back_populates='notification')
    targets: List['NotificationTarget'] = relationship('NotificationTarget', back_populates='notification')


class NotificationTarget(base.Base):
    __tablename__ = 'notification_target'

    # columns
    id = base.id_builder.build()
    notification_id = Column(base.id_builder.type, ForeignKey('notification.id'), nullable=False)
    target = Column(String(128), nullable=False)
    target_id = Column(String(128), nullable=False)
    action = Column(String(32))

    # associations
    notification: 'Notification' = relationship('Notification', back_populates='targets')

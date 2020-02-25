import enum
import json
from datetime import datetime
from typing import List, Optional

import pendulum
from sqlalchemy import Binary, String, Column, Float, ForeignKey, Boolean, DateTime, Index, Enum, UniqueConstraint, sql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from riberry import model
from riberry.model import base


class ResourceType(enum.Enum):
    application = 'Application'
    application_instance = 'ApplicationInstance'
    form = 'Form'
    job = 'Job'
    job_execution = 'JobExecution'
    job_execution_stream = 'JobExecutionStream'
    job_execution_stream_step = 'JobExecutionStreamStep'
    job_execution_artifact = 'JobExecutionArtifact'
    misc = 'Misc'
    user = 'User'
    user_interface = 'UserInterface'

    def __repr__(self):
        return repr(self.value)


class Document(base.Base):
    __tablename__ = 'document'
    __reprattrs__ = ['type']

    id = base.id_builder.build()
    type: str = Column(String(24), nullable=False, default='markdown')
    content: bytes = Column(Binary, nullable=False)


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
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now)
    read = Column(Boolean(name='notification_user_read'), nullable=False, default=False)

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


class MenuItem(base.Base):
    __tablename__ = 'menu_item'

    # columns
    id = base.id_builder.build()
    parent_id = Column(base.id_builder.type, ForeignKey('menu_item.id'))
    menu_type = Column(String(128), nullable=False)
    type = Column(String(128), nullable=False)
    key = Column(String(128), nullable=False)
    label: Optional[str] = Column(String(128), nullable=True)

    # associations
    parent: 'MenuItem' = relationship('MenuItem', back_populates='children', remote_side=[id])
    children: List['MenuItem'] = relationship('MenuItem', back_populates='parent')


class ResourceData(base.Base):
    __tablename__ = 'resource_data'
    __reprattrs__ = ['name']
    __table_args__ = (
        UniqueConstraint('resource_id', 'resource_type', 'name'),
    )

    # columns
    id = base.id_builder.build()
    resource_id = Column(base.id_builder.type, nullable=True)
    resource_type = Column(Enum(ResourceType), nullable=False)
    name: str = Column(String(256))
    raw_value: bytes = Column('value', Binary, nullable=True)
    lock: str = Column(String(72), nullable=True)
    expiry: datetime = Column(DateTime(timezone=True), nullable=True)
    marked_for_refresh: bool = Column(Boolean(name='resource_data_marked_for_refresh'), nullable=False, default=False)

    @hybrid_property
    def value(self):
        return json.loads(self.raw_value.decode()) if self.raw_value else None

    @value.setter
    def value(self, value):
        self.raw_value = json.dumps(value).encode()

    @classmethod
    def make_relationship(cls, resource_id, resource_type):
        return relationship(
            'ResourceData',
            primaryjoin=lambda: sql.and_(
                resource_id == ResourceData.resource_id,
                ResourceData.resource_type == resource_type
            ),
            order_by=lambda: ResourceData.id.asc(),
            foreign_keys=lambda: ResourceData.resource_id,
            cascade='save-update, merge, delete, delete-orphan',
        )


class ResourceRetention(base.Base):
    __tablename__ = 'resource_retention'
    __reprattrs__ = ['form_id', 'resource_type', 'period']

    id = base.id_builder.build()
    form_id = Column(base.id_builder.type, ForeignKey('form.id'), nullable=False)
    resource_type = Column(Enum(ResourceType), nullable=False)
    period = Column(String(64), nullable=False)
    last_completion_time: datetime = Column(DateTime(timezone=True), default=None)

    def run(self):
        """ Retrieves resources for the executions belonging to the given policy's form
        which have exceeded the retention period. """

        executions = self.ready_executions()
        if not executions:
            return
        execution_ids = [execution.id for execution in executions]

        if self.resource_type == ResourceType.job_execution_artifact:
            query = model.job.JobExecutionArtifact.query().filter(
                model.job.JobExecutionArtifact.job_execution_id.in_(execution_ids)
            )

        elif self.resource_type == ResourceType.job_execution_stream:
            query = model.job.JobExecutionStream.query().filter(
                model.job.JobExecutionStream.job_execution_id.in_(execution_ids)
            )

        elif self.resource_type == ResourceType.job_execution_stream_step:
            query = model.job.JobExecutionStreamStep.query().filter(
                model.job.JobExecutionStreamStep.stream_id.in_(
                    model.conn.query(model.job.JobExecutionStream.id).filter(
                        model.job.JobExecutionStream.job_execution_id.in_(execution_ids)
                    )
                )
            )

        elif self.resource_type == ResourceType.job_execution:
            query = model.job.JobExecution.query().filter(
                model.job.JobExecution.id.in_(execution_ids)
            )

        else:
            raise NotImplementedError(f'ResourceRetention.run :: Unsupported type {self.resource_type}')

        for resource in query.yield_per(10000):
            model.conn.delete(resource)

        self.last_completion_time = max(execution.completed for execution in executions)

    def ready_executions(self) -> List['model.job.JobExecution']:
        """ Retrieves executions for the given policy's form which have exceeded the retention period. """

        # noinspection PyComparisonWithNone
        query = model.job.JobExecution.query().join(model.job.Job).filter(
            (model.job.JobExecution.completed != None) &
            (model.job.JobExecution.completed < (base.utc_now() - pendulum.parse(self.period))) &
            (model.job.Job.form_id == self.form_id)
        )

        if self.last_completion_time:
            query = query.filter(model.job.JobExecution.completed > self.last_completion_time)

        return query.order_by(model.job.JobExecution.completed.asc()).all()

from datetime import datetime
from typing import List

import pendulum
from sqlalchemy import Column, String, Boolean, ForeignKey, DateTime
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from riberry import model
from riberry.model import base


class Application(base.Base):
    __tablename__ = 'application'
    __reprattrs__ = ['name', 'enabled']

    # columns
    id = base.id_builder.build()
    document_id = Column(base.id_builder.type, ForeignKey(column='document.id'))
    name: str = Column(String(64), nullable=False, unique=True)
    internal_name: str = Column(String(256), nullable=False, unique=True)
    description: str = Column(String(128))
    type: str = Column(String(64), nullable=False)
    enabled: bool = Column(Boolean, default=True)

    # associations
    instances: List['ApplicationInstance'] = relationship(
        'ApplicationInstance', back_populates='application')
    interfaces: List['model.interface.ApplicationInterface'] = relationship(
        'ApplicationInterface', back_populates='application')
    document: 'model.misc.Document' = relationship('Document')

    forms = relationship(
        'Form',
        secondary=lambda: ApplicationInstance.__table__,
        primaryjoin=lambda: Application.id == ApplicationInstance.application_id,
        secondaryjoin=lambda: ApplicationInstance.id == model.interface.Form.instance_id
    )


class ApplicationInstance(base.Base):
    __tablename__ = 'app_instance'
    __reprattrs__ = ['name', 'internal_name']

    # columns
    id = base.id_builder.build()
    application_id = Column(base.id_builder.type, ForeignKey(column='application.id'), nullable=False)
    name: str = Column(String(64), nullable=False, unique=True)
    internal_name: str = Column(String(256), nullable=False, unique=True)

    # associations
    application: 'Application' = relationship('Application', back_populates='instances')
    heartbeat: 'Heartbeat' = relationship('Heartbeat', uselist=False, back_populates='instance')
    schedules: List['ApplicationInstanceSchedule'] = relationship(
        'ApplicationInstanceSchedule', back_populates='instance')
    forms: List['model.interface.Form'] = relationship('Form', back_populates='instance')

    # proxies
    interfaces: List['model.interface.ApplicationInterface'] = association_proxy(
        target_collection='forms',
        attr='interface'
    )

    @property
    def status(self):

        schedules = self.schedules
        if schedules:
            for schedule in schedules:
                if not schedule.active():
                    return 'inactive'

        if not self.heartbeat:
            return 'created'

        diff = base.utc_now() - pendulum.instance(self.heartbeat.updated)
        if diff.seconds >= 10:
            return 'offline'
        return 'online'


class Heartbeat(base.Base):
    __tablename__ = 'heartbeat_app_instance'
    __reprattrs__ = ['instance_id', 'updated']

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'), nullable=False)
    created: datetime = Column(DateTime, default=base.utc_now)
    updated: datetime = Column(DateTime, default=base.utc_now)

    # associations
    instance: 'ApplicationInstance' = relationship('ApplicationInstance', back_populates='heartbeat')


class ApplicationInstanceSchedule(base.Base):
    __tablename__ = 'sched_app_instance'

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'), nullable=False)
    days = Column(String(27), nullable=False)
    start_time = Column(String(5), nullable=False)
    end_time = Column(String(5), nullable=False)
    timezone = Column(String(128), nullable=False, default='UTC')

    # associations
    instance: 'ApplicationInstance' = relationship('ApplicationInstance', back_populates='schedules')

    def active(self):
        now = pendulum.DateTime.now(tz=self.timezone)
        if self.days != '*':
            all_days = self.days.lower().split(',')
            if now.format('dd').lower() not in all_days:
                return False

        start_dt = pendulum.parse(self.start_time, tz=self.timezone)
        end_dt = pendulum.parse(self.end_time, tz=self.timezone)

        return end_dt > now > start_dt




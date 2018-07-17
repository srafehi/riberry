from datetime import datetime
from typing import List

import pendulum
from sqlalchemy import Column, String, Boolean, ForeignKey, DateTime, Integer, desc
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, validates

from riberry import model
from riberry.model import base


class Application(base.Base):
    """Contains basic metadata related to an application."""

    __tablename__ = 'application'
    __reprattrs__ = ['name', 'enabled']

    # columns
    id = base.id_builder.build()
    document_id = Column(base.id_builder.type, ForeignKey(column='document.id'))
    name: str = Column(String(64), nullable=False, unique=True, comment='The name of the application.')
    internal_name: str = Column(String(256), nullable=False, unique=True, comment='The internal name or secondary identifier of the application.')
    description: str = Column(String(256), name='A brief description of the application\'s purpose.')
    type: str = Column(String(64), nullable=False, comment='The type of application.')
    enabled: bool = Column(Boolean, default=True, comment='Whether or not this application and it\'s instances are enabled')

    # associations
    instances: List['ApplicationInstance'] = relationship(
        'ApplicationInstance', cascade='save-update, merge, delete, delete-orphan', back_populates='application')
    interfaces: List['model.interface.ApplicationInterface'] = relationship(
        'ApplicationInterface', cascade='save-update, merge, delete, delete-orphan', back_populates='application')
    document: 'model.misc.Document' = relationship('Document', cascade='save-update, merge, delete, delete-orphan', single_parent=True)

    forms: List['model.interface.Form'] = relationship(
        'Form',
        secondary=lambda: ApplicationInstance.__table__,
        primaryjoin=lambda: Application.id == ApplicationInstance.application_id,
        secondaryjoin=lambda: ApplicationInstance.id == model.interface.Form.instance_id,
        viewonly=True,
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
    heartbeat: 'Heartbeat' = relationship('Heartbeat', cascade='save-update, merge, delete, delete-orphan', uselist=False, back_populates='instance')
    schedules: List['ApplicationInstanceSchedule'] = relationship(
        'ApplicationInstanceSchedule',
        cascade='save-update, delete, delete-orphan',
        back_populates='instance',
        order_by=lambda: (
            ApplicationInstanceSchedule.parameter,
            desc(ApplicationInstanceSchedule.priority),
            ApplicationInstanceSchedule.start_time,
        )
    )
    forms: List['model.interface.Form'] = relationship('Form', cascade='save-update, merge, delete, delete-orphan', back_populates='instance')

    # proxies
    interfaces: List['model.interface.ApplicationInterface'] = association_proxy(
        target_collection='forms',
        attr='interface'
    )

    @property
    def status(self):

        if self.parameter('active', default='Y') == 'N':
            return 'inactive'

        if not self.heartbeat:
            return 'created'

        diff = base.utc_now() - pendulum.instance(self.heartbeat.updated)
        if diff.seconds >= 10:
            return 'offline'
        return 'online'

    def parameter(self, name, default=None, current_time=None):
        schedules = sorted(
            (s for s in self.schedules if s.parameter == name and s.active(current_time=current_time)),
            key=lambda s: (-s.priority, s.start_time)
        )

        for schedule in schedules:
            return schedule.value

        return default

    @property
    def parameters(self):
        parameters = {s.parameter for s in self.schedules}
        return {param: self.parameter(name=param) for param in parameters}


class Heartbeat(base.Base):
    __tablename__ = 'heartbeat_app_instance'
    __reprattrs__ = ['instance_id', 'updated']

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'), nullable=False)
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now)
    updated: datetime = Column(DateTime(timezone=True), default=base.utc_now)

    # associations
    instance: 'ApplicationInstance' = relationship('ApplicationInstance', back_populates='heartbeat')


class ApplicationInstanceSchedule(base.Base):
    __tablename__ = 'sched_app_instance'

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'), nullable=False)
    days = Column(String(27), nullable=False)
    start_time = Column(String(8), nullable=False, default='00:00:00')
    end_time = Column(String(8), nullable=False, default='23:59:59')
    timezone = Column(String(128), nullable=False, default='UTC')
    parameter = Column(String(32), nullable=False)
    value = Column(String(512), nullable=False)
    priority = Column(Integer, default=64, nullable=False)

    # associations
    instance: 'ApplicationInstance' = relationship('ApplicationInstance', back_populates='schedules')

    # validations
    @validates('priority')
    def validate_priority(self, _, priority):
        assert isinstance(priority, int) and 255 >= priority >= 1, (
            f'ApplicationInstanceSchedule.priority must be an integer between 1 and 255 (received {priority})')
        return priority

    @validates('start_time')
    def validate_start_time(self, _, start_time):
        return self.cleanse_time(time_=start_time)

    @validates('end_time')
    def validate_end_time(self, _, end_time):
        return self.cleanse_time(time_=end_time)

    @staticmethod
    def cleanse_time(time_):
        if isinstance(time_, int):
            now = pendulum.now()
            date = pendulum.DateTime(now.year, now.month, now.day) + pendulum.duration(seconds=time_)
            time_ = date.strftime('%H:%M:%S')

        if isinstance(time_, str):
            pendulum.DateTime.strptime(time_, '%H:%M:%S')
        return time_

    @validates('timezone')
    def validate_timezone(self, _, timezone):
        pendulum.timezone(timezone)
        return timezone

    @validates('days')
    def validate_days(self, _, days):
        if days in (None, '*'):
            return '*'

        days = [o.strip() for o in str(days).lower().split(',')]
        invalid_days = set(days) - {'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'}
        if invalid_days:
            raise ValueError(f'Invalid days received: {", ".join(invalid_days)}')
        return ','.join(days)

    def active(self, current_time=None):
        now: pendulum.DateTime = pendulum.instance(current_time) \
            if current_time else pendulum.DateTime.now(tz=self.timezone)
        now = now.replace(microsecond=0)

        if self.days != '*':
            all_days = self.days.lower().split(',')
            if now.format('dd').lower() not in all_days:
                return False

        start_dt = pendulum.parse(self.start_time, tz=self.timezone)\
            .replace(year=now.year, month=now.month, day=now.day)

        end_dt = pendulum.parse(self.end_time, tz=self.timezone)\
            .replace(year=now.year, month=now.month, day=now.day)

        return end_dt >= now >= start_dt

import enum
from datetime import datetime
from typing import List

import pendulum
from sqlalchemy import Column, String, Boolean, ForeignKey, DateTime, Integer, desc, Enum
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
    name: str = Column(String(64), nullable=False, unique=True, comment='The human-readable name of the application.')
    internal_name: str = Column(String(256), nullable=False, unique=True, comment='The internal name or secondary identifier of the application.')
    description: str = Column(String(256), comment='A brief description of the application\'s purpose.')
    type: str = Column(String(64), nullable=False, comment='The type of application.')
    enabled: bool = Column(Boolean(name='application_enabled'), default=True, comment='Whether or not this application and its instances are enabled (TODO).')

    # associations
    instances: List['ApplicationInstance'] = relationship(
        'ApplicationInstance', cascade='save-update, merge, delete, delete-orphan', back_populates='application')
    forms: List['model.interface.Form'] = relationship(
        'Form', cascade='save-update, merge, delete, delete-orphan', back_populates='application')
    document: 'model.misc.Document' = relationship('Document', cascade='save-update, merge, delete, delete-orphan', single_parent=True)


class ApplicationInstance(base.Base):
    """
    An ApplicationInstance represents a running instance of an Application. Multiple ApplicationInstances are useful
    when we want to separate out different types of executions for the same Application.

    An example of this is when we have an application which can support both long-running and short-running executions.
    We can spin up a separate instance with separate scheduling to ensure that the long-running jobs don't consume block
    the short-running jobs.
    """

    __tablename__ = 'app_instance'
    __reprattrs__ = ['name', 'internal_name']

    # columns
    id = base.id_builder.build()
    application_id = Column(base.id_builder.type, ForeignKey(column='application.id'), nullable=False)
    name: str = Column(String(64), nullable=False, unique=True, comment='The human-readable name of the application.')
    internal_name: str = Column(String(256), nullable=False, unique=True, comment='The internal name or secondary identifier of the application instance.')

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

    @property
    def status(self):
        if not self.heartbeat:
            return 'created'

        diff = base.utc_now() - pendulum.instance(self.heartbeat.updated)
        if diff.seconds >= 10:
            return 'offline'

        if self.active_schedule_value('active', default='Y') == 'N':
            return 'inactive'

        return 'online'

    def active_schedule_value(self, name, default=None, current_time=None):
        schedule = self.active_schedule(name=name, current_time=current_time)
        return schedule.value if schedule else default

    def active_schedule(self, name, current_time=None) -> 'ApplicationInstanceSchedule':
        schedules = sorted(
            (s for s in self.schedules if s.parameter == name and s.active(current_time=current_time)),
            key=lambda s: (-s.priority, s.start_time)
        )

        for schedule in schedules:
            return schedule

    @property
    def active_schedule_values(self):
        return {name: schedule.value if schedule else None for name, schedule in self.active_schedules.items()}

    @property
    def active_schedules(self):
        parameters = {s.parameter for s in self.schedules}
        return {param: self.active_schedule(name=param) for param in parameters}


class Heartbeat(base.Base):
    """
    The Heartbeat object is used by running ApplicationInstances to report back that they're alive. If an
    ApplicationInstance's last heartbeat was over 10 seconds ago, we assume that it is offline.
    """

    __tablename__ = 'heartbeat_app_instance'
    __reprattrs__ = ['instance_id', 'updated']

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'), nullable=False)
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, comment='The first heartbeat we received for the instance.')
    updated: datetime = Column(DateTime(timezone=True), default=base.utc_now, comment='The last heartbeat we received for the instance.')

    # associations
    instance: 'ApplicationInstance' = relationship('ApplicationInstance', back_populates='heartbeat')


class ApplicationInstanceSchedule(base.Base):
    """
    The ApplicationInstanceSchedule is a time-based schedule for dynamic parameters. It allows us to define parameters
    which change depending on the time of the day.

    There are currently two core parameters: `active` and `concurrency`. These allow us to automatically
    activate/de-activate and scale up/down our applications during different times of the day.

    Any custom parameter can be defined and consumed by our Celery application as long as we have a custom parameter
    handler in place which can interpret the parameter's values.
    """

    __tablename__ = 'sched_app_instance'

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'), nullable=False)
    days = Column(String(27), nullable=False, default='*', comment='Comma-separated list of specific days ("MON,WED"), or "*" for every day.')
    start_time = Column(String(8), nullable=False, default='00:00:00', comment='The time when this schedule activates.')
    end_time = Column(String(8), nullable=False, default='23:59:59', comment='The time when this schedule de-activates.')
    timezone = Column(String(128), nullable=False, default='UTC', comment='The timezone of for the given start and end times.')
    parameter = Column(String(32), nullable=False, comment='The parameter which this schedule applies to.')
    value = Column(String(512), comment='The value of the given parameter.')
    priority = Column(Integer, default=64, nullable=False, comment='Priority of the schedule, where higher values mean higher priority.')

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


class CapacityDistributionStrategy(enum.Enum):
    spread = 'spread'
    binpack = 'binpack'

    def __repr__(self):
        return repr(self.value)


class CapacityConfiguration(base.Base):

    __tablename__ = 'capacity_config'

    # columns
    id = base.id_builder.build()
    weight_parameter = Column(String(32), nullable=False, unique=True, comment='Parameter name which defines the requested capacity weight.')
    capacity_parameter = Column(String(32), nullable=False, comment='Parameter name which defines the total capacity allocation.')
    producer_parameter = Column(String(32), nullable=False, comment='Parameter name which defines the capacity producers.')
    distribution_strategy = Column(
        Enum(CapacityDistributionStrategy),
        nullable=False,
        default=CapacityDistributionStrategy.binpack,
        comment='Binpack (fill vertically) or spread (fill horizontally)'
    )

    # associations
    producers: List['CapacityProducer'] = relationship(
        'CapacityProducer',
        cascade='save-update, merge, delete, delete-orphan',
        back_populates='configuration'
    )


class CapacityProducer(base.Base):

    __tablename__ = 'capacity_producer'

    # columns
    id = base.id_builder.build()
    configuration_id = Column(base.id_builder.type, ForeignKey('capacity_config.id'), nullable=False)
    name = Column(String(128), nullable=False, comment='The human-readable name of the producer.')
    internal_name = Column(String(128), nullable=False, comment='The internal name of the producer.')
    capacity = Column(Integer, nullable=False, comment='The total capacity of the consumer.')

    # associations
    configuration: 'CapacityConfiguration' = relationship('CapacityConfiguration', back_populates='producers')

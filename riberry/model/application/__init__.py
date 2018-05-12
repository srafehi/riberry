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
    name: str = Column(String(64), nullable=False, unique=True)
    type: str = Column(String(64), nullable=False)
    module: str = Column(String(256), nullable=False, unique=True)
    enabled: bool = Column(Boolean, default=True)

    # associations
    instances: List['ApplicationInstance'] = relationship(
        'ApplicationInstance', back_populates='application')
    input_groups: List['model.input_group.InputGroupDefinition'] = relationship(
        'InputGroupDefinition', back_populates='application')


class ApplicationInstance(base.Base):
    __tablename__ = 'application_instance'
    __reprattrs__ = ['name', 'enabled']

    # columns
    id = base.id_builder.build()
    application_id = Column(base.id_builder.type, ForeignKey(column='application.id'), nullable=False)
    name: str = Column(String(64), nullable=False, unique=True)
    enabled: bool = Column(Boolean, default=True)

    # associations
    application: 'Application' = relationship('Application', back_populates='instances')
    heartbeat: 'Heartbeat' = relationship('Heartbeat', uselist=False, back_populates='instance')
    schedules: List['ApplicationInstanceSchedule'] = relationship(
        'ApplicationInstanceSchedule', back_populates='instance')
    input_group_associations: 'model.input_group.ApplicationInstanceInputGroup' = relationship(
        'ApplicationInstanceInputGroup', back_populates='instance')
    jobs: List['model.job.Job'] = relationship('Job', back_populates='instance')

    # proxies
    input_groups: List['model.input_group.InputGroupDefinition'] = association_proxy(
        target_collection="input_group_associations",
        attr="input_group_definition",
        creator=lambda input_group_definition: model.input_group.ApplicationInstanceInputGroup(
            input_group_definition=input_group_definition))

    @property
    def status(self):
        if not self.heartbeat:
            return 'created'

        diff = base.utc_now() - pendulum.instance(self.heartbeat.updated)
        if diff.seconds >= 10:
            return 'offline'
        return 'online'


class Heartbeat(base.Base):
    __tablename__ = 'heartbeat'
    __reprattrs__ = ['instance_id', 'updated']

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('application_instance.id'), nullable=False)
    created: datetime = Column(DateTime, default=base.utc_now)
    updated: datetime = Column(DateTime, default=base.utc_now)

    # associations
    instance: 'ApplicationInstance' = relationship('ApplicationInstance', back_populates='heartbeat')


class ApplicationInstanceSchedule(base.Base):
    __tablename__ = 'schedule_application_instance'

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('application_instance.id'), nullable=False)

    # associations
    instance: 'ApplicationInstance' = relationship('ApplicationInstance', back_populates='schedules')

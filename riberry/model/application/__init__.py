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


class ApplicationInstance(base.Base):
    __tablename__ = 'app_instance'
    __reprattrs__ = ['name', 'enabled']

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
    instance_interfaces: List['model.interface.ApplicationInstanceInterface'] = relationship(
        'ApplicationInstanceInterface', back_populates='instance')
    jobs: List['model.job.Job'] = relationship('Job', back_populates='instance')

    # proxies
    interfaces: List['model.interface.ApplicationInterface'] = association_proxy(
        target_collection='instance_interfaces',
        attr='interface'
    )

    @property
    def status(self):
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

    # associations
    instance: 'ApplicationInstance' = relationship('ApplicationInstance', back_populates='schedules')

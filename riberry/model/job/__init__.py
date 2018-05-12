from datetime import datetime
from typing import List

import pendulum
from sqlalchemy import Column, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from riberry import model
from riberry.model import base


class Job(base.Base):
    __tablename__ = 'job'
    __reprattrs__ = ['name']

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('application_instance.id'), nullable=False)
    creator_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    name: str = Column(String(64), nullable=False, unique=True)
    created: datetime = Column(DateTime, default=lambda: pendulum.now('utc'), nullable=False)

    # associations
    creator: 'model.auth.User' = relationship('User')
    instance: 'model.application.ApplicationInstance' = relationship('ApplicationInstance', back_populates='jobs')
    executions: List['JobExecution'] = relationship('JobExecution', back_populates='job')
    schedules: List['JobSchedule'] = relationship('JobSchedule', back_populates='job')
    input_group: 'model.input_group.InputGroupInstance' = relationship(
        'InputGroupInstance', uselist=False, back_populates='job')

    def execute(self):
        model.conn.add(instance=JobExecution(job=self))
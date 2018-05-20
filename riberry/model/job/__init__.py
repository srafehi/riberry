import mimetypes
from datetime import datetime
from typing import List

import pendulum
from croniter import croniter
from sqlalchemy import Column, String, ForeignKey, DateTime, Boolean, Integer, Binary
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, deferred
from riberry import model
from riberry.model import base


class Job(base.Base):
    __tablename__ = 'job'
    __reprattrs__ = ['name']

    # columns
    id = base.id_builder.build()
    instance_interface_id = Column(base.id_builder.type, ForeignKey('app_instance_interface.id'), nullable=False)
    creator_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    name: str = Column(String(64), nullable=False, unique=True)
    created: datetime = Column(DateTime, default=base.utc_now, nullable=False)

    # associations
    creator: 'model.auth.User' = relationship('User')
    instance_interface: 'model.interface.ApplicationInstanceInterface' = \
        relationship('ApplicationInstanceInterface', back_populates='jobs')
    executions: List['JobExecution'] = relationship('JobExecution', back_populates='job')
    schedules: List['JobSchedule'] = relationship('JobSchedule', back_populates='job')
    values: List['model.interface.InputValueInstance'] = relationship('InputValueInstance', back_populates='job')
    files: List['model.interface.InputFileInstance'] = relationship('InputFileInstance', back_populates='job')

    # proxies
    instance: 'model.interface.ApplicationInstanceInterface' = association_proxy('instance_interface', 'instance')

    def execute(self):
        model.conn.add(instance=JobExecution(job=self))


class JobSchedule(base.Base):
    __tablename__ = 'sched_job'

    # columns
    id = base.id_builder.build()
    job_id = Column(base.id_builder.type, ForeignKey('job.id'))
    job: 'Job' = relationship('Job', back_populates='schedules')
    enabled: bool = Column(Boolean, default=True, nullable=False)
    cron: str = Column(String(24), nullable=False)
    created: datetime = Column(DateTime, default=base.utc_now, nullable=False)
    last_run: datetime = Column(DateTime, default=None)
    limit: int = Column(Integer)
    total_runs: int = Column(Integer, default=0)

    def run(self):
        if not self.enabled:
            return

        ready_run = None
        for cron_time in croniter(self.cron, start_time=self.last_run or self.created, ret_type=pendulum.DateTime):
            cron_time = pendulum.instance(cron_time)
            if cron_time > base.utc_now():
                break
            ready_run = cron_time

        if ready_run:
            self.last_run = ready_run
            self.total_runs += 1
            if self.limit and self.total_runs >= self.limit:
                self.enabled = False

            self.job.execute()

    @property
    def next_run(self):
        if not self.enabled:
            return

        instance = croniter(self.cron, start_time=self.last_run or self.created, ret_type=pendulum.DateTime)
        return pendulum.instance(next(instance))


class JobExecution(base.Base):
    __tablename__ = 'job_execution'
    __reprattrs__ = ['job_id', 'task_id', 'status']

    # columns
    id = base.id_builder.build()
    job_id = Column(base.id_builder.type, ForeignKey('job.id'))
    job: 'Job' = relationship('Job', back_populates='executions')
    task_id: str = Column(String(36), unique=True)
    status: str = Column(String(24), default='RECEIVED')
    created: datetime = Column(DateTime, default=base.utc_now, nullable=False)
    started: datetime = Column(DateTime)
    completed: datetime = Column(DateTime)
    updated: datetime = Column(DateTime, default=base.utc_now, nullable=False)

    # associations
    streams: List['JobExecutionStream'] = relationship('JobExecutionStream', back_populates='job_execution')
    artifacts: List['JobExecutionArtifact'] = relationship('JobExecutionArtifact', back_populates='job_execution')


class JobExecutionStream(base.Base):
    __tablename__ = 'job_stream'
    __reprattrs__ = ['name', 'task_id', 'status']

    # columns
    id = base.id_builder.build()
    job_execution_id = Column(base.id_builder.type, ForeignKey('job_execution.id'), nullable=False)
    task_id: str = Column(String(36), unique=True)
    name: str = Column(String(64))
    status: str = Column(String(24), default='QUEUED')
    created: datetime = Column(DateTime, default=base.utc_now, nullable=False)
    started: datetime = Column(DateTime)
    completed: datetime = Column(DateTime)
    updated: datetime = Column(DateTime, default=base.utc_now, nullable=False)

    # associations
    job_execution: 'JobExecution' = relationship('JobExecution', back_populates='streams')
    steps: List['JobExecutionStreamStep'] = relationship('JobExecutionStreamStep', back_populates='stream')
    artifacts: List['JobExecutionArtifact'] = relationship('JobExecutionArtifact', back_populates='stream')


class JobExecutionStreamStep(base.Base):
    __tablename__ = 'job_stream_step'
    __reprattrs__ = ['name', 'task_id', 'status']

    # columns
    id = base.id_builder.build()
    stream_id = Column(base.id_builder.type, ForeignKey('job_stream.id'), nullable=False)
    task_id: str = Column(String(36), unique=True)
    name: str = Column(String(64))
    status: str = Column(String(24), default='RECEIVED')
    created: datetime = Column(DateTime, default=base.utc_now, nullable=False)
    started: datetime = Column(DateTime)
    completed: datetime = Column(DateTime)
    updated: datetime = Column(DateTime, default=base.utc_now, nullable=False)

    # associations
    stream: 'JobExecutionStream' = relationship('JobExecutionStream', back_populates='steps')


class JobExecutionArtifact(base.Base):
    __tablename__ = 'job_artifact'
    __reprattrs__ = ['name', 'filename']

    # columns
    id = base.id_builder.build()
    job_execution_id = Column(base.id_builder.type, ForeignKey('job_execution.id'), nullable=False)
    stream_id = Column(base.id_builder.type, ForeignKey('job_stream.id'))
    name: str = Column(String(64), nullable=False)
    type: str = Column(String(64), nullable=True)
    filename: str = Column(String(512), nullable=False)
    created: datetime = Column(DateTime, default=base.utc_now, nullable=False)
    size: int = Column(Integer, nullable=False)

    # associations
    job_execution: 'JobExecution' = relationship('JobExecution', back_populates='artifacts')
    stream: 'JobExecutionStream' = relationship('JobExecutionStream', back_populates='artifacts')

    @property
    def content_type(self):
        return mimetypes.guess_type(self.filename)[0]

    @property
    def content_encoding(self):
        return mimetypes.guess_type(self.filename)[1]


class JobExecutionArtifactBinary(base.Base):
    __tablename__ = 'job_artifact_binary'

    # columns
    id = base.id_builder.build()
    binary: bytes = deferred(Column(Binary, nullable=True))

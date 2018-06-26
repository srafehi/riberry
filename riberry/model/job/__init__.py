import enum
import mimetypes
from datetime import datetime
from typing import List

import pendulum
from croniter import croniter
from sqlalchemy import Column, String, ForeignKey, DateTime, Boolean, Integer, Binary, Index, Enum, desc
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, deferred, validates

from riberry import model
from riberry.model import base


class ArtifactType(enum.Enum):
    output = 'output'
    error = 'error'
    report = 'report'

    def __repr__(self):
        return repr(self.value)


class Job(base.Base):
    __tablename__ = 'job'
    __reprattrs__ = ['name']
    __table_args__ = (
        Index('j__idx_form_id', 'form_id'),
        Index('j__idx_creator_id', 'creator_id'),
    )

    # columns
    id = base.id_builder.build()
    form_id = Column(base.id_builder.type, ForeignKey('form.id'), nullable=False)
    creator_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    name: str = Column(String(64), nullable=False, unique=True)
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)

    # associations
    creator: 'model.auth.User' = relationship('User')
    form: 'model.interface.Form' = relationship('Form', back_populates='jobs')
    executions: List['JobExecution'] = relationship(
        'JobExecution',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: desc(JobExecution.updated),
        back_populates='job')
    schedules: List['JobSchedule'] = relationship('JobSchedule', cascade='save-update, merge, delete, delete-orphan', back_populates='job')
    values: List['model.interface.InputValueInstance'] = relationship('InputValueInstance', cascade='save-update, merge, delete, delete-orphan', back_populates='job')
    files: List['model.interface.InputFileInstance'] = relationship('InputFileInstance', cascade='save-update, merge, delete, delete-orphan', back_populates='job')

    # proxies
    instance: 'model.application.ApplicationInstance' = association_proxy('form', 'instance')
    interface: 'model.interface.ApplicationInterface' = association_proxy('form', 'interface')

    def execute(self, creator_id):
        model.conn.add(instance=JobExecution(job=self, creator_id=creator_id))


class JobSchedule(base.Base):
    __tablename__ = 'sched_job'

    # columns
    id = base.id_builder.build()
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)
    creator_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    enabled: bool = Column(Boolean, default=True, nullable=False)
    cron: str = Column(String(24), nullable=False)
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    last_run: datetime = Column(DateTime(timezone=True), default=None)
    limit: int = Column(Integer, default=0)
    total_runs: int = Column(Integer, default=0)

    # associations
    job: 'Job' = relationship('Job', back_populates='schedules')
    creator: 'model.auth.User' = relationship('User')

    def run(self):
        if not self.enabled or self.job.instance.status != 'online':
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

            self.job.execute(creator_id=self.creator_id)

    @property
    def next_run(self):
        if not self.enabled:
            return

        instance = croniter(self.cron, start_time=self.last_run or self.created, ret_type=pendulum.DateTime)
        return pendulum.instance(next(instance))


class JobExecution(base.Base):
    __tablename__ = 'job_execution'
    __reprattrs__ = ['job_id', 'task_id', 'status']
    __table_args__ = (
        Index('j_e__idx_job_id', 'job_id'),
        Index('j_e__idx_creator_id', 'creator_id'),
    )

    # columns
    id = base.id_builder.build()
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)
    creator_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    task_id: str = Column(String(36), unique=True)
    status: str = Column(String(24), default='RECEIVED')
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    started: datetime = Column(DateTime(timezone=True))
    completed: datetime = Column(DateTime(timezone=True))
    updated: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    priority = Column(Integer, default=64, nullable=False)

    # associations
    creator: 'model.auth.User' = relationship('User')
    job: 'Job' = relationship('Job', back_populates='executions')
    streams: List['JobExecutionStream'] = relationship(
        'JobExecutionStream',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: desc(JobExecutionStream.updated),
        back_populates='job_execution'
    )
    artifacts: List['JobExecutionArtifact'] = relationship(
        'JobExecutionArtifact',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: desc(JobExecutionArtifact.created),
        back_populates='job_execution')

    # validations
    @validates('priority')
    def validate_priority(self, _, priority):
        assert isinstance(priority, int) and 255 >= priority >= 1, (
            f'ApplicationInstanceSchedule.priority must be an integer between 1 and 255 (received {priority})')
        return priority


class JobExecutionStream(base.Base):
    __tablename__ = 'job_stream'
    __reprattrs__ = ['name', 'task_id', 'status']
    __table_args__ = (
        Index('j_s__idx_job_execution_id', 'job_execution_id'),
    )

    # columns
    id = base.id_builder.build()
    job_execution_id = Column(base.id_builder.type, ForeignKey('job_execution.id'), nullable=False)
    task_id: str = Column(String(36), unique=True)
    name: str = Column(String(64))
    status: str = Column(String(24), default='QUEUED')
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    started: datetime = Column(DateTime(timezone=True))
    completed: datetime = Column(DateTime(timezone=True))
    updated: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)

    # associations
    job_execution: 'JobExecution' = relationship('JobExecution', back_populates='streams')
    steps: List['JobExecutionStreamStep'] = relationship('JobExecutionStreamStep', cascade='save-update, merge, delete, delete-orphan', back_populates='stream')
    artifacts: List['JobExecutionArtifact'] = relationship('JobExecutionArtifact', back_populates='stream')


class JobExecutionStreamStep(base.Base):
    __tablename__ = 'job_stream_step'
    __reprattrs__ = ['name', 'task_id', 'status']
    __table_args__ = (
        Index('j_s_s__idx_stream_id', 'stream_id'),
    )

    # columns
    id = base.id_builder.build()
    stream_id = Column(base.id_builder.type, ForeignKey('job_stream.id'), nullable=False)
    task_id: str = Column(String(36), unique=True)
    name: str = Column(String(64))
    status: str = Column(String(24), default='RECEIVED')
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    started: datetime = Column(DateTime(timezone=True))
    completed: datetime = Column(DateTime(timezone=True))
    updated: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)

    # associations
    stream: 'JobExecutionStream' = relationship('JobExecutionStream', back_populates='steps')


class JobExecutionArtifact(base.Base):
    __tablename__ = 'job_artifact'
    __reprattrs__ = ['name', 'filename']
    __table_args__ = (
        Index('j_a__idx_job_execution_id', 'job_execution_id'),
        Index('j_a__idx_stream_id', 'stream_id'),
    )

    # columns
    id = base.id_builder.build()
    job_execution_id = Column(base.id_builder.type, ForeignKey('job_execution.id'), nullable=False)
    stream_id = Column(base.id_builder.type, ForeignKey('job_stream.id'))
    name: str = Column(String(128), nullable=False)
    type: str = Column(Enum(ArtifactType), nullable=False)
    category: str = Column(String(128), nullable=False, default='Default')
    filename: str = Column(String(512), nullable=False)
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    size: int = Column(Integer, nullable=False)

    # associations
    job_execution: 'JobExecution' = relationship('JobExecution', back_populates='artifacts')
    stream: 'JobExecutionStream' = relationship('JobExecutionStream', back_populates='artifacts')
    binary: 'JobExecutionArtifactBinary' = relationship(
        'JobExecutionArtifactBinary', cascade='save-update, merge, delete, delete-orphan', back_populates='artifact', uselist=False)
    data: List['JobExecutionArtifactData'] = relationship(
        'JobExecutionArtifactData', cascade='save-update, merge, delete, delete-orphan', back_populates='artifact')

    @property
    def content_type(self):
        return mimetypes.guess_type(self.filename)[0]

    @property
    def content_encoding(self):
        return mimetypes.guess_type(self.filename)[1]


class JobExecutionArtifactData(base.Base):
    __tablename__ = 'job_artifact_data'

    # columns
    id = base.id_builder.build()
    title: str = Column(String(64), nullable=False)
    description: str = Column(String(512), nullable=False)
    artifact_id = Column(base.id_builder.type, ForeignKey('job_artifact.id'), nullable=False)

    # associations
    artifact: 'JobExecutionArtifact' = relationship('JobExecutionArtifact', back_populates='data')


class JobExecutionArtifactBinary(base.Base):
    __tablename__ = 'job_artifact_binary'

    # columns
    id = base.id_builder.build()
    binary: bytes = deferred(Column(Binary, nullable=True))
    artifact_id = Column(base.id_builder.type, ForeignKey('job_artifact.id'), nullable=False)

    # associations
    artifact: 'JobExecutionArtifact' = relationship('JobExecutionArtifact', back_populates='binary')

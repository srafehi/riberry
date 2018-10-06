import enum
import mimetypes
from datetime import datetime
from typing import List

import pendulum
from croniter import croniter
from sqlalchemy import Column, String, ForeignKey, DateTime, Boolean, Integer, Binary, Index, Enum, desc, Float, asc
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
    """
    A Job is an object which represents a set of inputs provided to a form. A Job can have one or more JobExecutions
    which represent the execution of the linked ApplicationInterface on the linked ApplicationInstance for the given
    input stored against this Job.

    Jobs are immutable. If we require a different set of values, we'll need to create a new Job.
    """

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
    name: str = Column(String(64), nullable=False, unique=True, comment='The unique name of our job.')
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
    """
    A JobSchedule object defines a schedule which will trigger an execution for the linked Job.

    Note that a JobExecution will only be created for an ApplicationInstance which has a status of "online".
    Applications which are offline or inactive due to an ApplicationInstanceSchedule will not have JobExecutions
    created.
    """

    __tablename__ = 'sched_job'

    # columns
    id = base.id_builder.build()
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)
    creator_id = Column(base.id_builder.type, ForeignKey('users.id'), nullable=False)
    enabled: bool = Column(Boolean, default=True, nullable=False, comment='Whether or not this schedule is active.')
    cron: str = Column(String(24), nullable=False, comment='The cron expression which defines our schedule.')
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False, comment='The time our schedule was created.')
    last_run: datetime = Column(DateTime(timezone=True), default=None, comment='The last time a job execution was created from our schedule.')
    limit: int = Column(Integer, default=0, comment='The amount of valid runs for this schedule.')
    total_runs: int = Column(Integer, default=0, comment='The total amount of runs for this schedule.')

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
    """A JobExecution represent a single execution of our Job."""
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
    task_id: str = Column(String(36), unique=True, comment='The internal identifier of our job execution. This is usually the Celery root ID.')
    status: str = Column(String(24), default='RECEIVED', comment='The current status of our job execution.')
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    started: datetime = Column(DateTime(timezone=True))
    completed: datetime = Column(DateTime(timezone=True))
    updated: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    priority = Column(Integer, default=64, nullable=False, comment='The priority of this execution. This only applies to tasks in the RECEIVED state.')
    parent_execution_id = Column(base.id_builder.type, ForeignKey('job_execution.id'), comment='The id of the execution which triggered this execution.')

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
    progress: List['JobExecutionProgress'] = relationship(
        'JobExecutionProgress',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: asc(JobExecutionStream.id),
        back_populates='job_execution'
    )

    parent_execution: 'JobExecution' = relationship('JobExecution', back_populates='child_executions', remote_side=[id])
    child_executions: List['JobExecution'] = relationship('JobExecution', back_populates='parent_execution')

    # validations
    @validates('priority')
    def validate_priority(self, _, priority):
        assert isinstance(priority, int) and 255 >= priority >= 1, (
            f'ApplicationInstanceSchedule.priority must be an integer between 1 and 255 (received {priority})')
        return priority


class JobExecutionProgress(base.Base):
    __tablename__ = 'job_progress'
    __reprattrs__ = ['job_id', 'message']

    # columns
    id = base.id_builder.build()
    job_execution_id = Column(base.id_builder.type, ForeignKey('job_execution.id'), nullable=False)
    created: datetime = Column(DateTime(timezone=True), default=base.utc_now, nullable=False)
    message: str = Column(String(128), default=None, comment='Message describing the progress of the job execution.')
    progress_percentage = Column(Integer, default=None, nullable=True, comment='The progress of the job execution.')

    # associations
    job_execution: 'JobExecution' = relationship('JobExecution', back_populates='progress')

    # validations
    @validates('progress_percentage')
    def validate_priority(self, _, progress_percentage):
        if progress_percentage is not None:
            if progress_percentage < 0:
                progress_percentage = 0
            elif progress_percentage > 100:
                progress_percentage = 100
        return progress_percentage


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
        if self.filename.endswith('.log'):  # quick fix for missing .log type on unix systems
            return 'text/plain'

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

import json
import mimetypes
from typing import List

from sqlalchemy import Column, String, Boolean, ForeignKey, Integer, Binary, desc, asc, Text
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, deferred

from riberry import model
from riberry.model import base


class Form(base.Base):
    """
    A Form is an interface to creating jobs for a given ApplicationInstance.
    """

    __tablename__ = 'form'
    __reprattrs__ = ['internal_name', 'version']

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'), nullable=False)
    application_id = Column(base.id_builder.type, ForeignKey('application.id'), nullable=False)
    document_id = Column(base.id_builder.type, ForeignKey(column='document.id'))
    name: str = Column(String(64), unique=True, nullable=False, comment='The human-readable name of the form.')
    internal_name: str = Column(String(256), unique=True, nullable=False, comment='The internal name or secondary identifier of the form.')
    description: str = Column(String(256), comment='A brief description of the form\'s purpose.')
    version: int = Column(Integer, nullable=False, default=1, comment='The version of the form.')
    enabled: bool = Column(Boolean(name='form_enabled'), nullable=False, default=True, comment='Whether or not this form is enabled.')

    # associations
    instance: 'model.application.ApplicationInstance' = relationship('ApplicationInstance', back_populates='forms')
    application: 'model.application.Application' = relationship('Application', back_populates='forms')
    jobs: List['model.job.Job'] = relationship(
        'Job',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: desc(model.job.Job.created),
        back_populates='form'
    )
    job_executions: List['model.job.JobExecution'] = relationship(
        'JobExecution',
        secondary=lambda: model.job.Job.__table__,
        viewonly=True,
        sync_backref=False,
        back_populates='form',
    )
    job_schedules: List['model.job.JobSchedule'] = relationship(
        'JobSchedule',
        secondary=lambda: model.job.Job.__table__,
        viewonly=True,
        sync_backref=False,
        back_populates='form',
    )
    group_associations: List['model.group.ResourceGroupAssociation'] = model.group.ResourceGroupAssociation.make_relationship(
        resource_id=id,
        resource_type=model.misc.ResourceType.form
    )
    input_definition: 'InputDefinition' = relationship(
        'InputDefinition',
        cascade='save-update, merge, delete, delete-orphan',
        back_populates='form',
        uselist=False,
    )
    metrics: List['model.job.JobExecutionMetric'] = relationship(
        'JobExecutionMetric',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: asc(model.job.JobExecutionMetric.epoch_end),
        back_populates='form',
    )
    document: 'model.misc.Document' = relationship(
        'Document',
        cascade='save-update, merge, delete, delete-orphan',
        single_parent=True
    )

    # proxies
    groups: List['model.group.Group'] = association_proxy('group_associations', 'group')


class InputDefinition(base.Base):
    __tablename__ = 'input_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = base.id_builder.build()
    form_id = Column(base.id_builder.type, ForeignKey('form.id'), nullable=False)

    name: str = Column(String(64), nullable=False)
    type: str = Column(String(32), nullable=False, default='jsonschema')
    description: str = Column(String(256))
    definition_string: str = Column('definition', Text, nullable=False)

    # associations
    form: 'Form' = relationship('Form', back_populates='input_definition')

    @property
    def definition(self):
        return json.loads(self.definition_string) if self.definition_string else None

    @definition.setter
    def definition(self, value):
        self.definition_string = json.dumps(value, indent=2)


class InputValueInstance(base.Base):
    """The InputValueInstance object contains data for an InputDefinition and is linked to a Job."""

    __tablename__ = 'input_value_instance'
    __reprattrs__ = ['internal_name']

    # columns
    id = base.id_builder.build()
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)
    name: str = Column(String(256), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    value_string: str = Column('value', Text)

    # associations
    job: 'model.job.Job' = relationship('Job', back_populates='values')

    def __init__(self, value=None, **kwargs):
        super().__init__(**kwargs)
        if 'value_string' not in kwargs:
            self.value = value

    @property
    def value(self):
        return json.loads(self.value_string) if self.value_string else None

    @value.setter
    def value(self, value):
        self.value_string = json.dumps(value).encode()


class InputFileInstance(base.Base):
    """The InputFileInstance object represents a file in an InputDefinition and is linked to a Job."""

    __tablename__ = 'input_file_instance'
    __reprattrs__ = ['internal_name', 'filename', 'size']

    # columns
    id = base.id_builder.build()
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)
    name: str = Column(String(256), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    filename: str = Column(String(512), nullable=False)
    size: int = Column(Integer, nullable=False)
    binary: bytes = deferred(Column(Binary))

    # associations
    job: 'model.job.Job' = relationship('Job', back_populates='files')

    @property
    def content_type(self):
        if self.filename.endswith('.log'):  # quick fix for missing .log type on unix systems
            return 'text/plain'

        return mimetypes.guess_type(self.filename)[0]

    @property
    def content_encoding(self):
        return mimetypes.guess_type(self.filename)[1]

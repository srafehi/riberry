import json
import mimetypes
from typing import List

from sqlalchemy import Column, String, Boolean, ForeignKey, Integer, Binary, DateTime, desc
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
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
    schedules: List['FormSchedule'] = relationship('FormSchedule', back_populates='form')
    jobs: List['model.job.Job'] = relationship(
        'Job',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: desc(model.job.Job.created),
        back_populates='form'
    )
    group_associations: List['model.group.ResourceGroupAssociation'] = model.group.ResourceGroupAssociation.make_relationship(
        resource_id=id,
        resource_type=model.misc.ResourceType.form
    )
    input_value_definitions: List['InputValueDefinition'] = relationship(
        'InputValueDefinition',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: InputValueDefinition.id.asc(),
        back_populates='form'
    )
    input_file_definitions: List['InputFileDefinition'] = relationship(
        'InputFileDefinition',
        cascade='save-update, merge, delete, delete-orphan',
        order_by=lambda: InputFileDefinition.id.asc(),
        back_populates='form'
    )
    document: 'model.misc.Document' = relationship('Document', cascade='save-update, merge, delete, delete-orphan', single_parent=True)

    # proxies
    groups: List['model.group.Group'] = association_proxy('group_associations', 'group')


class FormSchedule(base.Base):
    __tablename__ = 'sched_form'

    # columns
    id = base.id_builder.build()
    form_id = Column(base.id_builder.type, ForeignKey('form.id'), nullable=False)
    start = Column(DateTime(timezone=True), nullable=False)
    end = Column(DateTime(timezone=True), nullable=False)

    # associations
    form: 'Form' = relationship('Form', back_populates='schedules')


class InputFileDefinition(base.Base):
    """The InputFileDefinition object defines the properties of an input file."""

    __tablename__ = 'input_file_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = base.id_builder.build()
    form_id = Column(base.id_builder.type, ForeignKey('form.id'), nullable=False)

    name: str = Column(String(64), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    description: str = Column(String(128))
    type: str = Column(String(64), nullable=False)
    accept: str = Column(String(256))
    required: bool = Column(Boolean(name='form_required'), nullable=False, default=True)

    # associations
    form: 'Form' = relationship('Form', back_populates='input_file_definitions')


class InputValueDefinition(base.Base):
    """The InputFileDefinition object defines the properties of an input value."""

    __tablename__ = 'input_value_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = base.id_builder.build()
    form_id = Column(base.id_builder.type, ForeignKey('form.id'), nullable=False)

    name: str = Column(String(64), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    description: str = Column(String(128))
    type: str = Column(String(64), nullable=False)
    required: bool = Column(Boolean(name='input_value_definition_required'), nullable=False, default=True)
    default_binary = Column('defaults', Binary)

    # associations
    form: 'Form' = relationship('Form', back_populates='input_value_definitions')
    allowed_value_enumerations: List['InputValueEnum'] = relationship(
        'InputValueEnum', cascade='save-update, merge, delete, delete-orphan', back_populates='definition')

    # proxies
    allowed_binaries: List[bytes] = association_proxy(
        'allowed_value_enumerations',
        'value',
        creator=lambda value: InputValueEnum(value=value)
    )

    @property
    def allowed_values(self):
        return [json.loads(v.decode()) for v in self.allowed_binaries]

    @hybrid_property
    def default_value(self):
        return json.loads(self.default_binary.decode()) if self.default_binary else None

    @default_value.setter
    def default_value(self, value):
        self.default_binary = json.dumps(value).encode()


class InputValueEnum(base.Base):
    """The InputValueEnum object defines a valid enumeration for a given InputValueInstance."""

    __tablename__ = 'input_value_enum'
    __reprattrs__ = ['value']

    # columns
    id = base.id_builder.build()
    definition_id = Column(base.id_builder.type, ForeignKey('input_value_definition.id'), nullable=False)
    value = Column(Binary, nullable=False)

    # associations
    definition: 'InputValueDefinition' = relationship(
        'InputValueDefinition', back_populates='allowed_value_enumerations')


class InputValueInstance(base.Base):
    """The InputValueInstance object contains data for a InputValueDefinition and is linked to a Job."""

    __tablename__ = 'input_value_instance'

    # columns
    id = base.id_builder.build()
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)
    name: str = Column(String(256), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    raw_value: bytes = Column('value', Binary)

    # associations
    job: 'model.job.Job' = relationship('Job', back_populates='values')

    @property
    def definition(self):
        return self.job.form

    @hybrid_property
    def value(self):
        return json.loads(self.raw_value.decode()) if self.raw_value else None

    @value.setter
    def value(self, value):
        self.raw_value = json.dumps(value).encode()


class InputFileInstance(base.Base):
    """The InputFileInstance object contains data for a InputFileDefinition and is linked to a Job."""

    __tablename__ = 'input_file_instance'
    __reprattrs__ = ['filename', 'size']

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

import json
import mimetypes
from typing import List

from sqlalchemy import Column, String, Boolean, ForeignKey, Integer, Binary, Time
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, deferred
from sqlalchemy.schema import UniqueConstraint

from riberry import model
from riberry.model import base


class ApplicationInterface(base.Base):
    __tablename__ = 'app_interface'
    __reprattrs__ = ['name', 'version']
    __table_args__ = (
        UniqueConstraint('internal_name', 'version', name='uc_name_version'),
    )

    # columns
    id = base.id_builder.build()
    application_id = Column(base.id_builder.type, ForeignKey('application.id'), nullable=False)
    document_id = Column(base.id_builder.type, ForeignKey(column='document.id'))
    name: str = Column(String(64), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    version: int = Column(Integer, nullable=False, default=1)
    description: str = Column(String(48))

    # associations
    application: 'model.application.Application' = relationship('Application', back_populates='interfaces')
    input_value_definitions: List['InputValueDefinition'] = relationship(
        'InputValueDefinition', back_populates='interface')
    input_file_definitions: List['InputFileDefinition'] = relationship(
        'InputFileDefinition', back_populates='interface')
    forms: List['Form'] = relationship('Form', back_populates='interface')
    document: 'model.misc.Document' = relationship('Document')

    # proxies
    application_instances: List['model.application.ApplicationInstance'] = association_proxy(
        'forms',
        'instance'
    )


class Form(base.Base):
    __tablename__ = 'form'
    __reprattrs__ = ['instance_id', 'interface_id']
    __table_args__ = (
        UniqueConstraint('instance_id', 'interface_id', name='uc_form'),
    )

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'))
    interface_id = Column(base.id_builder.type, ForeignKey('app_interface.id'))
    enabled: bool = Column(Boolean, nullable=False, default=True)

    # associations
    instance: 'model.application.ApplicationInstance' = relationship('ApplicationInstance', back_populates='forms')
    interface: 'ApplicationInterface' = relationship('ApplicationInterface', back_populates='forms', lazy='joined')
    schedules: List['FormSchedule'] = relationship('FormSchedule', back_populates='form')
    jobs: List['Job'] = relationship('Job', back_populates='form')
    group_associations: List['model.group.ResourceGroupAssociation'] = model.group.ResourceGroupAssociation.make_relationship(
        resource_id=id,
        resource_type=model.group.ResourceType.form
    )

    # proxies
    groups: List['model.group.Group'] = association_proxy('group_associations', 'group')


class FormSchedule(base.Base):
    __tablename__ = 'sched_form'

    # columns
    id = base.id_builder.build()
    form_id = Column(base.id_builder.type, ForeignKey('form.id'), nullable=False)
    start = Column(Time, nullable=False)
    end = Column(Time, nullable=False)

    # associations
    form: 'Form' = relationship('Form', back_populates='schedules')


class InputFileDefinition(base.Base):
    __tablename__ = 'input_file_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = base.id_builder.build()
    application_interface_id = Column(base.id_builder.type, ForeignKey('app_interface.id'))

    name: str = Column(String(64), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    description: str = Column(String(48))
    type: str = Column(String(64), nullable=False)
    required: bool = Column(Boolean, nullable=False, default=True)

    # associations
    interface: 'ApplicationInterface' = relationship('ApplicationInterface', back_populates='input_file_definitions')


class InputValueDefinition(base.Base):
    __tablename__ = 'input_value_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = base.id_builder.build()
    application_interface_id = Column(base.id_builder.type, ForeignKey('app_interface.id'))

    name: str = Column(String(64), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    description: str = Column(String(48))
    type: str = Column(String(64), nullable=False)
    required: bool = Column(Boolean, nullable=False, default=True)
    default_binary = Column('default', Binary)

    # associations
    interface: 'ApplicationInterface' = relationship('ApplicationInterface', back_populates='input_value_definitions')
    allowed_value_enumerations: List['InputValueEnum'] = relationship('InputValueEnum', back_populates='definition')

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
    __tablename__ = 'input_value_enum'
    __reprattrs__ = ['value']

    # columns
    id = base.id_builder.build()
    definition_id = Column(base.id_builder.type, ForeignKey('input_value_definition.id'), nullable=False)
    value = Column(Binary, nullable=False)

    # associations
    definition: 'InputValueDefinition' = relationship('InputValueDefinition', back_populates='allowed_value_enumerations')


class InputValueInstance(base.Base):
    __tablename__ = 'input_value_instance'

    # columns
    id = base.id_builder.build()
    definition_id = Column(base.id_builder.type, ForeignKey('input_value_definition.id'), nullable=False)
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)
    raw_value: bytes = Column('value', Binary)

    # associations
    job: 'model.job.Job' = relationship('Job', back_populates='values')
    definition: 'InputValueDefinition' = relationship('InputValueDefinition')

    @hybrid_property
    def value(self):
        return json.loads(self.raw_value.decode()) if self.raw_value else None

    @value.setter
    def value(self, value):
        self.raw_value = json.dumps(value).encode()


class InputFileInstance(base.Base):
    __tablename__ = 'input_file_instance'
    __reprattrs__ = ['filename', 'size']

    # columns
    id = base.id_builder.build()
    filename: str = Column(String(512), nullable=False)
    definition_id = Column(base.id_builder.type, ForeignKey('input_file_definition.id'), nullable=False)
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)
    size: int = Column(Integer, nullable=False)
    binary: bytes = deferred(Column(Binary))

    # associations
    job: 'model.job.Job' = relationship('Job', back_populates='files')
    definition: 'InputFileDefinition' = relationship('InputFileDefinition')

    @property
    def content_type(self):
        return mimetypes.guess_type(self.filename)[0]

    @property
    def content_encoding(self):
        return mimetypes.guess_type(self.filename)[1]

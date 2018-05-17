import csv
import io
import json
import mimetypes
from typing import List, Dict, Optional, AnyStr

import yaml
from sqlalchemy import Column, String, Boolean, ForeignKey, Integer, Binary
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, deferred
from sqlalchemy.schema import UniqueConstraint

from riberry import model
from riberry.model import base


class ApplicationInterface(base.Base):
    __tablename__ = 'app_interface'
    __reprattrs__ = ['name', 'version']

    # columns
    id = base.id_builder.build()
    application_id = Column(base.id_builder.type, ForeignKey('application.id'), nullable=False)
    document_id = Column(base.id_builder.type, ForeignKey(column='document.id'))
    name: str = Column(String(64), nullable=False)
    internal_name: str = Column(String(256), nullable=False, unique=True)
    version: int = Column(Integer, nullable=False, default=1)
    description: str = Column(String(48))

    # associations
    application: 'application.Application' = relationship('Application', back_populates='interfaces')
    input_value_definitions: List['InputValueDefinition'] = relationship(
        'InputValueDefinition', back_populates='interface')
    input_file_definitions: List['InputFileDefinition'] = relationship(
        'InputFileDefinition', back_populates='interface')
    instance_interfaces: List['ApplicationInstanceInterface'] = relationship(
        'ApplicationInstanceInterface', back_populates='interface')
    document: 'model.misc.Document' = relationship('Document')

    # proxies
    application_instances: List['model.application.ApplicationInstance'] = association_proxy(
        'instance_interfaces',
        'instance'
    )


class ApplicationInstanceInterface(base.Base):
    __tablename__ = 'app_instance_interface'
    __reprattrs__ = ['instance_id', 'interface_id']
    __table_args__ = (
        UniqueConstraint('instance_id', 'interface_id', name='uc_instance_interface'),
    )

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance.id'))
    interface_id = Column(base.id_builder.type, ForeignKey('app_interface.id'))
    enabled: bool = Column(Boolean, nullable=False, default=True)

    # associations
    instance: 'model.application.ApplicationInstance' = relationship(
        'ApplicationInstance', back_populates='instance_interfaces')
    interface: 'ApplicationInterface' = relationship(
        'ApplicationInterface', back_populates='instance_interfaces', lazy='joined')
    schedules: List['ApplicationInstanceInterfaceSchedule'] = relationship(
        'ApplicationInstanceInterfaceSchedule', back_populates='instance_interface')
    inputs: List['JobInput'] = relationship('JobInput', back_populates='instance_interface')

    # proxies
    jobs: List['model.job.Job'] = association_proxy('inputs', 'job')


class ApplicationInstanceInterfaceSchedule(base.Base):
    __tablename__ = 'sched_app_instance_interface'

    # columns
    id = base.id_builder.build()
    instance_id = Column(base.id_builder.type, ForeignKey('app_instance_interface.id'), nullable=False)

    # associations
    instance_interface: 'ApplicationInstanceInterface' = relationship(
        'ApplicationInstanceInterface', back_populates='schedules')


class InputFileDefinition(base.Base):
    __tablename__ = 'input_file_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = base.id_builder.build()
    input_group_id = Column(base.id_builder.type, ForeignKey('app_interface.id'))

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
    input_group_id = Column(base.id_builder.type, ForeignKey('app_interface.id'))

    name: str = Column(String(64), nullable=False)
    internal_name: str = Column(String(256), nullable=False)
    description: str = Column(String(48))
    type: str = Column(String(64), nullable=False)
    required: bool = Column(Boolean, nullable=False, default=True)
    default_binary = Column('default', Binary)

    # associations
    interface: 'ApplicationInterface' = relationship('ApplicationInterface', back_populates='input_value_definitions')
    allowed_values: List['InputValueEnum'] = relationship('InputValueEnum', back_populates='definition')

    @hybrid_property
    def default_value(self):
        return yaml.load(self.default_binary.decode()) if self.default_binary else None

    @default_value.setter
    def default_value(self, value):
        self.default_binary = yaml.dump(value).encode()


class InputValueEnum(base.Base):
    __tablename__ = 'input_value_enum'
    __reprattrs__ = ['value']

    # columns
    id = base.id_builder.build()
    definition_id = Column(base.id_builder.type, ForeignKey('input_value_definition.id'), nullable=False)
    value = Column(Binary, nullable=False)

    # associations
    definition: 'InputValueDefinition' = relationship('InputValueDefinition', back_populates='allowed_values')


class JobInput(base.Base):
    __tablename__ = 'job_input'

    # columns
    id = base.id_builder.build()
    instance_interface_id = Column(base.id_builder.type, ForeignKey('app_instance_interface.id'), nullable=False)
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)

    # associations
    values: List['InputValueInstance'] = relationship('InputValueInstance', back_populates='job_input')
    files: List['InputFileInstance'] = relationship('InputFileInstance', back_populates='job_input')
    instance_interface: 'ApplicationInstanceInterface' = relationship('ApplicationInstanceInterface')
    job: 'model.job.Job' = relationship('Job', back_populates='input')

    # proxies
    interface: 'ApplicationInterface' = association_proxy('instance_interface', 'interface')

    def inputs_dict(self):
        value_map_definitions: Dict[str, 'InputValueDefinition'] = {
            input_def.name: input_def for input_def in self.interface.input_value_definitions}
        file_map_definitions: Dict[str, 'InputFileDefinition'] = {
            input_def.name: input_def for input_def in self.interface.input_file_definitions}

        value_ins_definitions: Dict[str, 'InputValueInstance'] = {
            input_ins.definition.name: input_ins for input_ins in self.values}
        file_ins_definitions: Dict[str, 'InputFileInstance'] = {
            input_ins.definition.name: input_ins for input_ins in self.files}

        output = {}
        for name, definition in value_map_definitions.items():
            instance = value_ins_definitions.get(name)
            raw_value: bytes = instance.raw_value if instance else None
            if not raw_value:
                if definition.required:
                    raise ValueError(f'Mandatory input '
                                     f'{repr(definition.name)}/{repr(definition.parameter)} not provided for {self}')
                raw_value = definition.default_binary
            output[definition.internal_name] = self._deserialize_value(raw_value, definition.type)

        for name, definition in file_map_definitions.items():
            instance = file_ins_definitions.get(name)
            binary: bytes = instance.binary if instance else None
            if not binary:
                if definition.required:
                    raise ValueError(f'Mandatory input file '
                                     f'{repr(definition.name)}/{repr(definition.parameter)} not provided for {self}')
            output[definition.internal_name] = self._deserialize_value(binary, definition.type)

        return output

    @staticmethod
    def _deserialize_value(value, value_type):
        if value_type in ('int', 'bool', 'str', 'integer', 'boolean', 'string',
                          'text', 'number',
                          'float', 'json', 'yaml'):
            return yaml.load(value)
        if value_type == 'csv':
            reader = csv.DictReader(io.StringIO(value.decode()))
            return list(reader)

        return value.decode()


class InputValueInstance(base.Base):
    __tablename__ = 'input_value_instance'

    # columns
    id = base.id_builder.build()
    definition_id = Column(base.id_builder.type, ForeignKey('input_value_definition.id'), nullable=False)
    job_input_id = Column(base.id_builder.type, ForeignKey('job_input.id'), nullable=False)
    raw_value: bytes = Column('value', Binary)

    # associations
    job_input: 'JobInput' = relationship('JobInput', back_populates='values')
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
    job_input_id = Column(base.id_builder.type, ForeignKey('job_input.id'), nullable=False)
    size: int = Column(Integer, nullable=False)
    binary: bytes = deferred(Column(Binary))

    # associations
    job_input: 'JobInput' = relationship('JobInput', back_populates='files')
    definition: 'InputFileDefinition' = relationship('InputFileDefinition')

    @property
    def content_type(self):
        return mimetypes.guess_type(self.filename)[0]

    @property
    def content_encoding(self):
        return mimetypes.guess_type(self.filename)[1]

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

from riberry import model
from riberry.model import base


class InputGroupDefinition(base.Base):
    __tablename__ = 'input_group_definition'
    __reprattrs__ = ['name', 'version']

    # columns
    id = base.id_builder.build()
    application_id = Column(base.id_builder.type, ForeignKey('application.id'), nullable=False)
    document_id = Column(base.id_builder.type, ForeignKey(column='document.id'))
    name: str = Column(String(64), nullable=False)
    version: int = Column(Integer, nullable=False, default=1)
    description: str = Column(String(48))

    # associations
    application: 'application.Application' = relationship('Application', back_populates='input_groups')
    input_definitions: List['InputValueDefinition'] = relationship(
        'InputValueDefinition', back_populates='input_group_definition')
    input_file_definitions: List['InputFileDefinition'] = relationship(
        'InputFileDefinition', back_populates='input_group_definition')
    instance_associations: List['ApplicationInstanceInputGroup'] = relationship(
        'ApplicationInstanceInputGroup', back_populates='input_group_definition')
    document: 'model.misc.Document' = relationship('Document')

    # proxies
    application_instances: List['application.ApplicationInstance'] = association_proxy(
        'instance_associations',
        'instance',
        creator=lambda instance: ApplicationInstanceInputGroup(instance=instance))


class ApplicationInstanceInputGroup(base.Base):
    __tablename__ = 'appl_instance_input_group'
    __reprattrs__ = ['instance_id', 'input_group_id']

    # columns
    instance_id = Column(base.id_builder.type, ForeignKey('application_instance.id'), primary_key=True)
    input_group_id = Column(base.id_builder.type, ForeignKey('input_group_definition.id'), primary_key=True)
    enabled: bool = Column(Boolean, nullable=False, default=True)

    # associations
    instance: 'model.application.ApplicationInstance' = relationship(
        'ApplicationInstance', back_populates='input_group_associations')
    input_group_definition: 'InputGroupDefinition' = relationship(
        'InputGroupDefinition', back_populates='instance_associations', lazy='joined')


class InputFileDefinition(base.Base):
    __tablename__ = 'input_file_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = base.id_builder.build()
    input_group_id = Column(base.id_builder.type, ForeignKey('input_group_definition.id'))

    name: str = Column(String(64), nullable=False)
    description: str = Column(String(48))
    parameter: str = Column(String(64), nullable=False)
    type: str = Column(String(64), nullable=False)
    required: bool = Column(Boolean, nullable=False, default=True)

    # associations
    input_group_definition: 'InputGroupDefinition' = relationship(
        'InputGroupDefinition', back_populates='input_file_definitions')


class InputValueDefinition(base.Base):
    __tablename__ = 'input_value_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = base.id_builder.build()
    input_group_id = Column(base.id_builder.type, ForeignKey('input_group_definition.id'))

    name: str = Column(String(64), nullable=False)
    description: str = Column(String(48))
    parameter: str = Column(String(64), nullable=False)
    type: str = Column(String(64), nullable=False)
    required: bool = Column(Boolean, nullable=False, default=True)
    raw_default_value = Column('default_value', Binary)

    # associations
    input_group_definition: 'InputGroupDefinition' = relationship(
        'InputGroupDefinition', back_populates='input_definitions')

    @hybrid_property
    def default_value(self):
        return yaml.load(self.raw_default_value.decode()) if self.raw_default_value else None

    @default_value.setter
    def default_value(self, value):
        self.raw_default_value = yaml.dump(value).encode()


class InputGroupInstance(base.Base):
    __tablename__ = 'input_group_instance'

    # columns
    id = base.id_builder.build()
    definition_id = Column(base.id_builder.type, ForeignKey('input_group_definition.id'), nullable=False)
    job_id = Column(base.id_builder.type, ForeignKey('job.id'), nullable=False)

    # associations
    inputs: List['InputValueInstance'] = relationship('InputValueInstance', back_populates='input_group')
    input_files: List['InputFileInstance'] = relationship('InputFileInstance', back_populates='input_group')
    definition: 'InputGroupDefinition' = relationship('InputGroupDefinition')
    job: 'model.job.Job' = relationship('Job', back_populates='input_group')

    def inputs_dict(self):
        value_map_definitions: Dict[str, 'InputValueDefinition'] = {
            input_def.name: input_def for input_def in self.definition.input_definitions}
        file_map_definitions: Dict[str, 'InputFileDefinition'] = {
            input_def.name: input_def for input_def in self.definition.input_file_definitions}

        value_ins_definitions: Dict[str, 'InputValueInstance'] = {
            input_ins.definition.name: input_ins for input_ins in self.inputs}
        file_ins_definitions: Dict[str, 'InputFileInstance'] = {
            input_ins.definition.name: input_ins for input_ins in self.input_files}

        output = {}
        for name, definition in value_map_definitions.items():
            instance = value_ins_definitions.get(name)
            raw_value: bytes = instance.raw_value if instance else None
            if not raw_value:
                if definition.required:
                    raise ValueError(f'Mandatory input '
                                     f'{repr(definition.name)}/{repr(definition.parameter)} not provided for {self}')
                raw_value = definition.raw_default_value
            output[definition.parameter] = self._deserialize_value(raw_value, definition.type)

        for name, definition in file_map_definitions.items():
            instance = file_ins_definitions.get(name)
            binary: bytes = instance.binary if instance else None
            if not binary:
                if definition.required:
                    raise ValueError(f'Mandatory input file '
                                     f'{repr(definition.name)}/{repr(definition.parameter)} not provided for {self}')
            output[definition.parameter] = self._deserialize_value(binary, definition.type)

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
    input_group_instance_id = Column(base.id_builder.type, ForeignKey('input_group_instance.id'), nullable=False)
    raw_value: bytes = Column('value', Binary)

    # associations
    input_group: 'InputGroupInstance' = relationship('InputGroupInstance', back_populates='inputs')
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
    input_group_instance_id = Column(base.id_builder.type, ForeignKey('input_group_instance.id'), nullable=False)
    size: int = Column(Integer, nullable=False)
    binary: bytes = deferred(Column(Binary))

    # associations
    input_group: 'InputGroupInstance' = relationship('InputGroupInstance', back_populates='input_files')
    definition: 'InputFileDefinition' = relationship('InputFileDefinition')

    @property
    def content_type(self):
        return mimetypes.guess_type(self.filename)[0]

    @property
    def content_encoding(self):
        return mimetypes.guess_type(self.filename)[1]

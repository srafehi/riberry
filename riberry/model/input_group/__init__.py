import mimetypes
from typing import List

import yaml
from sqlalchemy import Column, String, Boolean, ForeignKey, Integer, Binary
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from ..base import Base, id_builder
from .. import application


class InputGroupDefinition(Base):
    __tablename__ = 'input_group_definition'
    __reprattrs__ = ['name', 'version']

    # columns
    id = id_builder.build()
    application_id = Column(id_builder.type, ForeignKey('application.id'), nullable=False)
    name: str = Column(String(64), nullable=False)
    version: int = Column(Integer, nullable=False, default=1)

    # associations
    application: 'application.Application' = relationship('Application', back_populates='input_groups')
    input_definitions: List['InputValueDefinition'] = relationship(
        'InputValueDefinition', back_populates='input_group_definition')
    input_file_definitions: List['InputFileDefinition'] = relationship(
        'InputFileDefinition', back_populates='input_group_definition')
    instance_associations: List['ApplicationInstanceInputGroup'] = relationship(
        'ApplicationInstanceInputGroup', back_populates='input_group_definition')

    # proxies
    application_instances: List['application.ApplicationInstance'] = association_proxy(
        'instance_associations',
        'instance',
        creator=lambda instance: ApplicationInstanceInputGroup(instance=instance))


class ApplicationInstanceInputGroup(Base):
    __tablename__ = 'appl_instance_input_group'
    __reprattrs__ = ['instance_id', 'input_group_id']

    # columns
    instance_id = Column(id_builder.type, ForeignKey('application_instance.id'), primary_key=True)
    input_group_id = Column(id_builder.type, ForeignKey('input_group_definition.id'), primary_key=True)
    enabled: bool = Column(Boolean, nullable=False, default=True)

    # associations
    instance: 'application.ApplicationInstance' = relationship('ApplicationInstance', back_populates='input_group_associations')
    input_group_definition: 'InputGroupDefinition' = relationship('InputGroupDefinition', back_populates='instance_associations', lazy='joined')


class InputFileDefinition(Base):
    __tablename__ = 'input_file_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = id_builder.build()
    input_group_id = Column(id_builder.type, ForeignKey('input_group_definition.id'))

    name: str = Column(String(64), nullable=False)
    parameter: str = Column(String(64), nullable=False)
    type: str = Column(String(64), nullable=False)
    required: bool = Column(Boolean, nullable=False, default=True)

    # associations
    input_group_definition: 'InputGroupDefinition' = relationship('InputGroupDefinition', back_populates='input_file_definitions')

    @property
    def content_type(self):
        return mimetypes.guess_type(self.filename)[0]

    @property
    def content_encoding(self):
        return mimetypes.guess_type(self.filename)[1]


class InputValueDefinition(Base):
    __tablename__ = 'input_value_definition'
    __reprattrs__ = ['name', 'type']

    # columns
    id = id_builder.build()
    input_group_id = Column(id_builder.type, ForeignKey('input_group_definition.id'))

    name: str = Column(String(64), nullable=False)
    parameter: str = Column(String(64), nullable=False)
    type: str = Column(String(64), nullable=False)
    required: bool = Column(Boolean, nullable=False, default=True)
    raw_default_value = Column('default_value', Binary)

    # associations
    input_group_definition: 'InputGroupDefinition' = relationship('InputGroupDefinition', back_populates='input_definitions')

    @hybrid_property
    def default_value(self):
        return yaml.load(self.raw_default_value.decode()) if self.raw_default_value else None

    @default_value.setter
    def default_value(self, value):
        self.raw_default_value = yaml.dump(value).encode()
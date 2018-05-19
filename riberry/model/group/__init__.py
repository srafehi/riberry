import enum
from typing import List

from sqlalchemy import String, Column, Enum, ForeignKey, sql
from sqlalchemy.orm import relationship

from riberry import model
from riberry.model import base


class ResourceType(enum.Enum):
    application_instance_interface = 'ApplicationInstanceInterface'
    user = 'User'


class Group(base.Base):
    __tablename__ = 'group'
    __reprattrs__ = ['name']

    # columns
    id = base.id_builder.build()
    name: str = Column(String(64), nullable=False)

    # associations
    resource_associations: List['ResourceGroupAssociation'] = relationship(
        'ResourceGroupAssociation', back_populates='group')


class ResourceGroupAssociation(base.Base):
    __tablename__ = 'resource_group'

    # columns
    id = base.id_builder.build()
    group_id = Column(ForeignKey('group.id'), nullable=False)
    resource_id = Column(base.id_builder.type, nullable=False)
    resource_type = Column(Enum(ResourceType), nullable=False)

    # associations
    group: 'Group' = relationship('Group', back_populates='resource_associations')

    @classmethod
    def make_relationship(cls, resource_id, resource_type):
        return relationship(
            'ResourceGroupAssociation',
            primaryjoin=lambda: sql.and_(
                resource_id == model.group.ResourceGroupAssociation.resource_id,
                model.group.ResourceGroupAssociation.resource_type == resource_type
            ),
            foreign_keys=lambda: model.group.ResourceGroupAssociation.resource_id
        )


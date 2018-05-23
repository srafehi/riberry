import enum
from typing import List

from sqlalchemy import String, Column, Enum, ForeignKey, sql
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from riberry import model
from riberry.model import base


class ResourceType(enum.Enum):
    application_instance_interface = 'ApplicationInstanceInterface'
    user = 'User'

    def __repr__(self):
        return repr(self.value)


class ResourceGroupAssociation(base.Base):
    __tablename__ = 'resource_group'
    __reprattrs__ = ['group_id', 'resource_id', 'resource_type']

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
                resource_id == ResourceGroupAssociation.resource_id,
                ResourceGroupAssociation.resource_type == resource_type
            ),
            foreign_keys=lambda: ResourceGroupAssociation.resource_id
        )


class Group(base.Base):
    __tablename__ = 'group'
    __reprattrs__ = ['name']

    # columns
    id = base.id_builder.build()
    name: str = Column(String(64), nullable=False, unique=True)

    # associations
    resource_associations: List['ResourceGroupAssociation'] = relationship(
        'ResourceGroupAssociation', back_populates='group')
    user_associations: List['ResourceGroupAssociation'] = relationship(
        'ResourceGroupAssociation',
        primaryjoin=lambda: sql.and_(
            ResourceGroupAssociation.group_id == Group.id,
            ResourceGroupAssociation.resource_type == ResourceType.user
        )
    )
    instance_interface_associations: List['ResourceGroupAssociation'] = relationship(
        'ResourceGroupAssociation',
        primaryjoin=lambda: sql.and_(
            ResourceGroupAssociation.group_id == Group.id,
            ResourceGroupAssociation.resource_type == ResourceType.application_instance_interface
        )
    )

    @property
    def users(self):
        return model.auth.User.query().filter(
            (ResourceGroupAssociation.group_id == self.id) &
            (ResourceGroupAssociation.resource_type == ResourceType.user) &
            (model.auth.User.id == ResourceGroupAssociation.resource_id)
        ).all()

    @property
    def instance_interfaces(self):
        return model.interface.ApplicationInstanceInterface.query().filter(
            (ResourceGroupAssociation.group_id == self.id) &
            (ResourceGroupAssociation.resource_type == ResourceType.application_instance_interface) &
            (model.interface.ApplicationInstanceInterface.id == ResourceGroupAssociation.resource_id)
        ).all()





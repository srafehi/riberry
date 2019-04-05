import enum
from typing import List

from sqlalchemy import String, Column, Enum, ForeignKey, sql
from sqlalchemy.orm import relationship

from riberry import model
from riberry.model import base


class ResourceGroupAssociation(base.Base):
    __tablename__ = 'resource_group'
    __reprattrs__ = ['group_id', 'resource_id', 'resource_type']

    # columns
    id = base.id_builder.build()
    group_id = Column(ForeignKey('groups.id'), nullable=False)
    resource_id = Column(base.id_builder.type, nullable=False)
    resource_type = Column(Enum(model.misc.ResourceType), nullable=False)

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
            foreign_keys=lambda: ResourceGroupAssociation.resource_id,
            cascade='save-update, merge, delete, delete-orphan',
        )


class Group(base.Base):
    __tablename__ = 'groups'
    __reprattrs__ = ['name']

    # columns
    id = base.id_builder.build()
    name: str = Column(String(128), nullable=False, unique=True)
    _display_name: str = Column('display_name', String(128))
    description: str = Column(String(128))

    # associations
    resource_associations: List['ResourceGroupAssociation'] = relationship(
        'ResourceGroupAssociation', back_populates='group')
    user_associations: List['ResourceGroupAssociation'] = relationship(
        'ResourceGroupAssociation',
        primaryjoin=lambda: sql.and_(
            ResourceGroupAssociation.group_id == Group.id,
            ResourceGroupAssociation.resource_type == model.misc.ResourceType.user
        )
    )
    form_associations: List['ResourceGroupAssociation'] = relationship(
        'ResourceGroupAssociation',
        primaryjoin=lambda: sql.and_(
            ResourceGroupAssociation.group_id == Group.id,
            ResourceGroupAssociation.resource_type == model.misc.ResourceType.form
        )
    )

    @property
    def display_name(self):
        return self._display_name or self.name

    @property
    def users(self):
        return model.auth.User.query().filter(
            (ResourceGroupAssociation.group_id == self.id) &
            (ResourceGroupAssociation.resource_type == model.misc.ResourceType.user) &
            (model.auth.User.id == ResourceGroupAssociation.resource_id)
        ).all()

    @property
    def forms(self):
        return model.interface.Form.query().filter(
            (ResourceGroupAssociation.group_id == self.id) &
            (ResourceGroupAssociation.resource_type == model.misc.ResourceType.form) &
            (model.interface.Form.id == ResourceGroupAssociation.resource_id)
        ).all()





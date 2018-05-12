from sqlalchemy import Binary, String, Column, Float

from riberry.model import base
from riberry.model.base import id_builder


class Document(base.Base):
    __tablename__ = 'document'

    id = id_builder.build()
    type = Column(String(24), nullable=False, default='markdown')
    content = Column(Binary, nullable=False)


class Event(base.Base):
    __tablename__ = 'event'
    __reprattrs__ = ['name', 'root_id']

    # columns
    id = id_builder.build()
    name: str = Column(String(64), nullable=False)
    time: float = Column(Float, nullable=False)
    root_id: str = Column(String(36), nullable=False)
    task_id: str = Column(String(36), nullable=False)
    data: str = Column(String(1024))
    binary: bytes = Column(Binary)

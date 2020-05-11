from typing import Any

from sqlalchemy import ColumnDefault
from sqlalchemy.orm.attributes import InstrumentedAttribute


def max_string_length(attribute: InstrumentedAttribute) -> int:
    """ Returns the max allowed length for a given SQLAlchemy attribute. """
    
    return attribute.property.columns[0].type.length


def has_default_value(attribute: InstrumentedAttribute) -> bool:
    """ Returns True if a default value is defined for the given SQLAlchemy attribute. """

    default = attribute.property.columns[0].default
    return isinstance(default, ColumnDefault)


def default_value(attribute: InstrumentedAttribute) -> Any:
    """ Returns the default value for the given SQLAlchemy attribute. """

    default = attribute.property.columns[0].default
    return default.arg if isinstance(default, ColumnDefault) else None

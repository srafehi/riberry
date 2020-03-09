

def max_string_length(attribute) -> int:
    """ Returns the max allowed length for a given SQLAlchemy attribute. """
    
    return attribute.property.columns[0].type.length

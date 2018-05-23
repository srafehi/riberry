from webargs import fields as w
from marshmallow import fields as m


base_args = {
    'expand': w.DelimitedList(m.Str(), missing=tuple()),
    'offset': m.Int(missing=0),
    'limit': m.Int(missing=10)
}


def _merge(source, destination):
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            _merge(value, node)
        else:
            destination[key] = value

    return destination


def _expand(attr):
    result = attr.split('.', maxsplit=1)
    return {result[0]: {} if len(result) == 1 else _expand(result[1])}


def parse_args(options):

    expansions = {}
    for expansion in options['expand']:
        _merge(_expand(expansion), expansions)
    options['expand'] = expansions
    return options
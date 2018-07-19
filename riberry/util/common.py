import os
from string import Template


def variable_substitution(obj):
    if isinstance(obj, str):
        try:
            return Template(template=obj).substitute(os.environ)
        except KeyError as exc:
            key = exc.args[0]
            raise ValueError(f'Environment variable substitution failed for {key!r}. '
                             f'Does the environment variable exist?')
    elif isinstance(obj, dict):
        return {k: variable_substitution(v) for k, v in obj.items()}
    elif isinstance(obj, (tuple, list, set)):
        return type(obj)(map(variable_substitution, obj))
    else:
        return obj

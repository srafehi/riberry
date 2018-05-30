from . import defaults
import importlib
import pkgutil
from collections import defaultdict


plugin_register = defaultdict(set)
plugin_register['authentication'].add(defaults.authentication.DefaultAuthenticationProvider)


ext_plugins = {
    name: importlib.import_module(name)
    for finder, name, ispkg in pkgutil.iter_modules()
    if name.startswith('riberry_')
}

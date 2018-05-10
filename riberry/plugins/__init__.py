from . import default
import importlib
import pkgutil

ext_plugins = {
    name: importlib.import_module(name)
    for finder, name, ispkg
    in pkgutil.iter_modules()
    if name.startswith('riberry_')
}
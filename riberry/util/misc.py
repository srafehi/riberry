import importlib


def import_from_string(module_path: str):
    if ':' in module_path:
        module_path, object_name = module_path.split(':')
    else:
        object_name = None

    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError:
        print(f'Could not find module {module_path}.')

    return getattr(mod, object_name) if object_name else mod

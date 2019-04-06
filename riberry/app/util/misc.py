import inspect


class Proxy:

    def __init__(self, getter):
        self.get = getter

    def __getattr__(self, item):
        return getattr(self.get(), item)

    def __repr__(self):
        return f'Proxy({self.get()})'


def function_path(func):
    return f'{inspect.getmodule(func).__name__}.{func.__name__}'


def internal_data_key(key):
    return f'_internal:{key}'

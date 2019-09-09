import importlib

import click

from ..root import cli


@cli.command('web')
@click.option('--module', '-m', show_default=True, default='riberry_web:main', help='Callable to start the web server.')
@click.option('--host', '-h', show_default=True, default='127.0.0.1', help='Bind to given host.')
@click.option('--port', '-p', show_default=True, default=5445, help='Bind to given port.')
@click.option('--log-level', '-l', show_default=True, default='info', help='Logging level.')
@click.pass_context
def run_webapp(cxt, module: str, host: str, port: int, log_level: str):
    module_dot_path, callable_name = module.split(':')
    try:
        mod = importlib.import_module(module_dot_path)
    except ModuleNotFoundError:
        print(f'Could not find module {module_dot_path}.')
        return cxt.exit(1)

    try:
        callable_obj = getattr(mod, callable_name)
    except AttributeError:
        print(f'Could not find callable {callable_name} in module {module_dot_path}.')
        return cxt.exit(1)

    callable_obj(host=host, port=port, log_level=log_level)

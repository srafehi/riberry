import importlib
import os

import click

import riberry
from riberry.app.backends.impl.pool.base import RiberryPoolBackend
from riberry.app.backends.impl.pool.log import configure as log_configure
from ..root import cli


@click.group()
def run():
    pass


@run.command()
@click.option('--module', '-m', required=True, help='Module containing Riberry pool application')
@click.option('--instance', '-i', help='Riberry application instance to run')
@click.option('--log-level', '-l', default='ERROR', help='Log level')
@click.option('--concurrency', '-c', default=None, help='Task concurrency', type=int)
def pool(module, instance, log_level, concurrency):
    if instance is not None:
        os.environ['RIBERRY_INSTANCE'] = instance

    log_configure(log_level=log_level)
    importlib.import_module(module)
    backend: RiberryPoolBackend = riberry.app.current_riberry_app.backend
    backend.task_queue.limit = concurrency
    backend.start()


cli.add_command(run)

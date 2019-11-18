import shutil

import click

import riberry
from ..root import cli


@cli.command('conf', help='Copy the given environment config to Riberry\'s default path')
@click.argument('path', required=True)
def import_config(path):
    target = str(riberry.config.CONF_DEFAULT_PATH)
    shutil.copy(path, target)
    print(f'Copied {path!r} to {target!r}.')

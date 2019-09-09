import shutil

import click

import riberry
from ..root import cli


@cli.command('conf')
@click.argument('path')
def import_config(path):
    target = str(riberry.config.CONF_DEFAULT_PATH)
    shutil.copy(path, target)
    print(f'Copied {path!r} to {target!r}.')

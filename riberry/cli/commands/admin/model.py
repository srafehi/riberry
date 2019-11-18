import json

import click

from riberry.util import config_importer
from .base import admin


@click.group(help='Collection of model function')
def model():
    pass


@model.command('import', help='Imports and synchronises Riberry\'s models from config')
@click.option('--config-path', '-p', prompt='Configuration path', help='YAML file containing riberry model.')
@click.option('--commit/--rollback', '-c/-r', prompt='Dry run', is_flag=True,
              help='Gathers all database changes without commits.', default=False)
@click.option('--app', '-a', 'apps', help='Restricts the import to the specified app', multiple=True)
def importer(config_path, commit, apps):
    changes = config_importer.import_from_file(config_path, dry_run=not commit, restrict_apps=apps)
    print(json.dumps(changes, indent=2))


admin.add_command(model)

import json

import click

from . import user, config_importer


@click.group()
def cli():
    pass


@cli.command('import')
@click.option('--config-path', '-p', prompt='Configuration path', help='YAML file containing application references.')
@click.option('--commit/--rollback', '-c/-r', prompt='Dry run', is_flag=True,
              help='Gathers all database changes without commits.', default=False)
def importer(config_path, commit):
    changes = config_importer.import_from_file(config_path, dry_run=not commit)
    print(json.dumps(changes, indent=2))


@cli.command('add-user')
@click.option('--username', prompt='Username', help="User's username")
@click.option('--password', prompt='Password', help="User's password", hide_input=True, confirmation_prompt=True)
@click.option('--first-name', prompt='First name', help="User's first name")
@click.option('--last-name', prompt='Last name', help="User's last name")
@click.option('--display-name', prompt='Full name', help="User's last name")
@click.option('--department', prompt='Department', help="User's department")
@click.option('--email', prompt='Email', help="User's email")
def add_user(username, password, first_name, last_name, display_name, department, email):
    user_id = user.add_user(
        username=username,
        password=password,
        first_name=first_name,
        last_name=last_name,
        display_name=display_name,
        department=department,
        email=email,
    )
    print(f'Created user {username} (User ID: {user_id})')


if __name__ == '__main__':
    cli()

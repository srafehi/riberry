import json

import click

from . import user, config_importer, groups


@click.group()
def cli():
    pass


@cli.command('import')
@click.option('--config-path', '-p', prompt='Configuration path', help='YAML file containing application references.')
@click.option('--commit/--rollback', '-c/-r', prompt='Dry run', is_flag=True,
              help='Gathers all database changes without commits.', default=False)
@click.option('--app', '-a', 'apps', help='Restricts the import to the specified app', multiple=True)
def importer(config_path, commit, apps):
    changes = config_importer.import_from_file(config_path, dry_run=not commit, restrict_apps=apps)
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


@cli.command('user-groups')
@click.argument('action', type=click.Choice(['add', 'remove']))
@click.option('--username', '-u', prompt='Username', help="User's username")
@click.option('--group', '-g', prompt='Username', help="Group's name")
def modify_user_groups(action, username, group):
    try:
        if action == 'add':
            groups.add_user_to_group(username=username, group_name=group)
            print(f'Added {username} to group {group}')
        elif action == 'remove':
            groups.remove_user_from_group(username=username, group_name=group)
            print(f'Removed {username} from group {group}')
    except Exception as exc:
        print(str(exc))
        exit(1)


if __name__ == '__main__':
    cli()

import click

from riberry.util import groups
from .base import admin


@click.group(help='Collection of group function')
def group():
    pass


@group.command('add', help='Add a user to a group')
@click.option('--username', '-u', prompt='Username', help="User's username")
@click.option('--group', '-g', prompt='Group', help="Group's name")
def add(username, group):
    try:
        groups.add_user_to_group(username=username, group_name=group)
        print(f'Added {username} to group {group}')
    except Exception as exc:
        print(str(exc))
        exit(1)


@group.command('remove', help='Remove a user from a group')
@click.option('--username', '-u', prompt='Username', help="User's username")
@click.option('--group', '-g', prompt='Group', help="Group's name")
def remove(username, group):
    try:
        groups.remove_user_from_group(username=username, group_name=group)
        print(f'Removed {username} from group {group}')
    except Exception as exc:
        print(str(exc))
        exit(1)


admin.add_command(group)

import click

from riberry.util.__main__ import add_user
from .base import admin


@click.group(help='Collection of user function')
def user():
    pass


user.add_command(add_user, name='create')
admin.add_command(user)

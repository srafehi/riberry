from . import sync, user, group
from .base import admin
from ...root import cli

cli.add_command(admin)

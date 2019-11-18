from riberry.util.__main__ import importer
from .base import admin

admin.add_command(importer, name='sync')

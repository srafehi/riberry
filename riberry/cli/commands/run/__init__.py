from . import core, pool, web
from .base import run
from ...root import cli

cli.add_command(run)

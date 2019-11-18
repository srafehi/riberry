from .base import run
from . import core, pool, web
from ...root import cli


cli.add_command(run)

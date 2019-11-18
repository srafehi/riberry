from .base import run
from . import core, pool
from ...root import cli


cli.add_command(run)

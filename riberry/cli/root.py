import click

import riberry


@click.group()
@click.version_option(riberry.__version__, prog_name='riberry')
def cli():
    pass

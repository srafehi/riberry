import click
from ..root import cli


@click.group()
def run():
    pass


@run.command()
@click.pass_context
def pool(ctx: click.Context):
    print('not implemented')
    ctx.exit(1)


cli.add_command(run)

import atexit
import subprocess

import click

from riberry import log
from .base import run

log = log.make(__name__)


def _kill(processes):
    for process in processes:
        try:
            if process.poll() is None:
                process.kill()
        except Exception:
            log.exception(f'Failed to kill process {process}')


@run.command(help='Start Riberry\'s core background celery app')
@click.option('--log-level', '-l', default='ERROR', help='Log level')
def core(log_level):
    process_beat = subprocess.Popen([
        'celery', 'beat',
        '-A', 'riberry.celery.background',
        '-l', log_level.lower(),
    ])

    process_background = subprocess.Popen([
        'celery', 'worker',
        '-A', 'riberry.celery.background',
        '-l', log_level.lower(),
        '-c', '1',
        '-Q', 'riberry.background.custom,riberry.background.schedules,riberry.background.events',
    ])

    processes = process_background, process_beat

    atexit.register(lambda: _kill(processes=processes))

    try:
        process_background.wait()
        process_beat.wait()
    except KeyboardInterrupt:
        log.info('Exiting riberry core...')

import logging
import os
import sys
import threading

import riberry


class Filter(logging.Filter):

    def filter(self, record):
        if riberry.app.current_context.current.root_id:
            record.root_id = f'root={riberry.app.current_context.current.root_id}'
        else:
            record.root_id = '-'

        if riberry.app.current_context.current.task_id:
            record.prefix = f'[id={riberry.app.current_context.current.task_id}] '
        else:
            record.prefix = ''

        if riberry.app.current_context.current.step:
            record.context = f'{riberry.app.current_context.current.step}'
        elif threading.current_thread() == threading.main_thread():
            record.context = 'backend.main'
        else:
            record.context = record.threadName

        return record


def configure(log_level='ERROR'):
    handler = riberry.log.stdout_handler
    handler.formatter = logging.Formatter(
        os.environ.get(
            'RIBERRY_LOGFORMAT',
            '%(levelname)-8s | %(asctime)-s | %(context)-41s | %(root_id)-41s | %(prefix)s%(message)s'
        )
    )

    for filter_ in list(handler.filters):
        handler.removeFilter(filter=filter_)

    handler.addFilter(Filter())
    riberry.log.logger.addHandler(handler)
    riberry.log.logger.setLevel(log_level.upper())

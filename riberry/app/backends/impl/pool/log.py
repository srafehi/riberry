import logging
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
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(levelname)-8s | %(asctime)-s | %(context)-41s | %(root_id)-41s | %(prefix)s%(message)s')
    )
    handler.addFilter(Filter())
    riberry.log.root.addHandler(handler)
    riberry.log.root.setLevel(log_level.upper())

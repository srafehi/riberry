from threading import Event

import riberry
from .background import background
from .executor import execution_listener
from .external_task_receiver import queue_receiver_tasks

log = riberry.log.make(__name__)


def run_task(name, func, interval, exit_event: Event):
    log.debug('Started task %s', name)

    while not exit_event.is_set():
        try:
            func()
        except:
            log.exception('Error occurred while processing task %s', name)
        finally:
            if interval:
                exit_event.wait(interval)

    log.debug('Stopped task %s', name)

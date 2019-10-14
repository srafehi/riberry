import logging

import riberry
from ..task_queue import TaskQueue

log = logging.getLogger(__name__)


def background(queue: TaskQueue):
    riberry.app.tasks.echo()
    with queue.lock:
        if not queue.limit_reached():
            riberry.app.tasks.poll(track_executions=True, filter_func=lambda _: not queue.limit_reached())
        else:
            log.debug('Queue limit reached, skipped task polling')

    riberry.app.tasks.refresh()

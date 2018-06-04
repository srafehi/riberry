import os
from urllib.parse import urlparse

import redis
from celery import current_app

from riberry import model


def instance_schedule():
    with model.conn:
        instance_name = os.environ['RIBERRY_INSTANCE']
        instance: model.application.ApplicationInstance = model.application.ApplicationInstance.query().filter_by(
            internal_name=instance_name
        ).first()

        return instance.status == 'inactive' if instance else True


def check_queues(queues):
    broker_uri = current_app.connection().as_uri(include_password=True)
    url = urlparse(broker_uri)
    r = redis.Redis(host=url.hostname, port=url.port, password=url.password)

    separator = '\x06\x16'
    priority_steps = [0, 3, 6, 9]
    for queue in queues:
        for prio in priority_steps:
            queue = f'{queue}{separator}{prio}' if prio else queue
            queue_length = r.llen(queue)
            if queue_length:
                return False

    if any([sum(r.values()) for r in current_app.control.broadcast('worker_task_count', reply=True)]):
        return False

    return True

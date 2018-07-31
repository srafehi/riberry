from urllib.parse import urlparse

import redis
from celery import current_app


def celery_redis_instance():
    broker_uri = current_app.connection().as_uri(include_password=True)
    url = urlparse(broker_uri)
    return redis.Redis(host=url.hostname, port=url.port, password=url.password)

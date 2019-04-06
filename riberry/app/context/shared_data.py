import datetime
from collections import Mapping, defaultdict
from contextlib import contextmanager
from operator import itemgetter
from typing import AnyStr, Any, Dict, Iterator

import pendulum
import time
from sqlalchemy.exc import IntegrityError

import riberry


class SharedExecutionData(Mapping):

    def __init__(self, context):
        self.context: riberry.app.context.Context = context
        self._lock: Dict[AnyStr, riberry.model.misc.ResourceData] = {}
        self._dirty = set()
        self._listeners = defaultdict(list)

    def listen(self, key, callback):
        self._listeners[key].append(callback)

    def __getitem__(self, item: AnyStr) -> Any:
        return self._get_instance(key=item).value

    def __setitem__(self, key, value):
        if key not in self._lock:
            raise PermissionError(f'No lock present for data key {key!r}')

        if pendulum.DateTime.utcnow() > pendulum.instance(self._lock[key].expiry):
            raise TimeoutError(f'Lock for data key {key!r} has expired!')

        self._lock[key].value = value
        self._dirty.add(key)

    def __delitem__(self, key):
        instance = self._get_instance(key=key)
        riberry.model.conn.delete(instance=instance)
        riberry.model.conn.commit()

    def __len__(self) -> int:
        return riberry.model.misc.ResourceData.query().filter_by(
            resource_id=self.context.current.job_execution.id,
            resource_type=riberry.model.misc.ResourceType.job_execution,
        ).count()

    def __iter__(self) -> Iterator[AnyStr]:
        instances = riberry.model.conn.query(
            riberry.model.misc.ResourceData.name
        ).filter_by(
            resource_id=self.context.current.job_execution.id,
            resource_type=riberry.model.misc.ResourceType.job_execution,
        ).all()

        return map(itemgetter(0), instances)

    def _get_instance(self, key):
        job_execution = self.context.current.job_execution

        # check if key exists
        instance = riberry.model.misc.ResourceData.query().filter_by(
            resource_id=job_execution.id,
            resource_type=riberry.model.misc.ResourceType.job_execution,
            name=key,
        ).first()

        # create if it doesn't
        if not instance:
            instance = riberry.model.misc.ResourceData(
                resource_id=job_execution.id,
                resource_type=riberry.model.misc.ResourceType.job_execution,
                name=key
            )
            try:
                riberry.model.conn.add(instance)
                riberry.model.conn.commit()
            except IntegrityError:
                riberry.model.conn.rollback()
                return self._get_instance(key=key)

        return instance

    def _acquire_lock(self, key, ttl, poll_interval):
        instance = self._get_instance(key=key)
        lock_value = self.context.current.task_id

        while True:
            riberry.model.conn.query(riberry.model.misc.ResourceData).filter(
                (riberry.model.misc.ResourceData.id == instance.id) &
                (
                    (riberry.model.misc.ResourceData.lock == None) |
                    (riberry.model.misc.ResourceData.expiry < datetime.datetime.now(tz=datetime.timezone.utc))
                )
            ).update({
                'lock': lock_value,
                'expiry': pendulum.DateTime.utcnow().add(seconds=ttl)
            })
            riberry.model.conn.commit()
            riberry.model.conn.expire(instance=instance)

            if instance.lock != lock_value:
                time.sleep(poll_interval)
            else:
                self._lock[key] = instance
                break

    def _release_lock(self, key):
        if self._lock:
            instance = self._lock.pop(key)
            instance.lock = None
            instance.expiry = None
            instance.marked_for_refresh = instance.marked_for_refresh or (key in self._dirty)
            riberry.model.conn.commit()

            if key in self._dirty:
                for listener in self._listeners[key]:
                    listener(key)
                self._dirty.remove(key)

    @contextmanager
    def lock(self, key, ttl=60, poll_interval=1):
        try:
            yield self._acquire_lock(key=key, ttl=ttl, poll_interval=poll_interval)
        finally:
            self._release_lock(key=key)

    def execute_once(self, key, func):
        with self.context.data.lock(key=key):
            if not self.context.data[key]:
                self.context.data[key] = True
                func()

    def set(self, key, value, **kwargs):
        with self.lock(key=key, **kwargs):
            self[key] = value() if callable(value) else value

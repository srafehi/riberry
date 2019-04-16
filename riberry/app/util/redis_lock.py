import time

from redis.exceptions import LockError


class RedisLock:

    def __init__(self, name, on_acquired, interval, min_interval=100):
        self.name = name
        self.on_acquired = on_acquired
        self.interval = interval
        self.min_interval = min_interval

    @property
    def key_timeout(self):
        return f'lock:{self.name}:timeout'

    @property
    def key_lock(self):
        return f'lock:{self.name}:acquire'

    def run(self, redis_instance):

        if redis_instance.get(name=self.key_timeout) is None:
            try:
                self._attempt_lock(redis_instance=redis_instance)
            except LockError:
                pass

    def _attempt_lock(self, redis_instance):
        with redis_instance.lock(name=self.key_lock, timeout=60, blocking_timeout=0.0):
            print(f"{self.name}: acquired lock...")
            time_start = time.time()
            try:
                self.on_acquired()
            finally:
                process_time = time.time() - time_start
                self._set_timeout(redis_instance=redis_instance, process_time=process_time)

    def _set_timeout(self, redis_instance, process_time):
        expiry = int(max(self.interval - (process_time * 1000), self.min_interval))
        redis_instance.set(name=self.key_timeout, value='1', px=expiry)
        print(f'{self.name}: processed task in {process_time:03}, setting lock expiry to {expiry:03} milliseconds')

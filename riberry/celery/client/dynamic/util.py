import time
import redis
import functools
from celery.utils.log import logger


class PriorityQueue:

    def __init__(
            self, r: redis.Redis, key: str, prefix: str='pq', sep: str=':', blocking: bool=True, block_retry: int=0.5):
        self.r: redis.Redis = r
        self.key = key
        self.prefix = prefix
        self.sep = sep
        self.blocking = blocking
        self.block_retry = block_retry

    def make_key(self, *args):
        sub_key = self.sep.join(map(str, args))
        return f'{self.prefix}{self.sep}{sub_key}'

    @property
    def version(self):
        return int(self.r.get(self.version_key) or 0)

    @version.setter
    def version(self, value):
        self.r.set(self.version_key, value=value)

    @property
    def version_key(self):
        return self.make_key(self.key, 'counter')

    @property
    def free_key(self):
        return self.generate_free_key(version=self.version)

    @property
    def lease_key(self):
        return self.generate_lease_key(version=self.version)

    def generate_free_key(self, version):
        return self.make_key(self.key, f'{version:09}', 'free')

    def generate_lease_key(self, version):
        return self.make_key(self.key, f'{version:09}', 'lease')

    def pop(self):
        while True:
            version = self.version
            result = self.r.transaction(
                functools.partial(self.pop_transaction, version=version),
                self.generate_free_key(version=version),
                self.generate_lease_key(version=version),
                value_from_callable=True
            )
            if not result and self.blocking:
                logger.warn(f'PriorityQueue: ({self.free_key}) encountered blank key, '
                            f'retrying after {self.block_retry} seconds.')
                time.sleep(self.block_retry)
                continue
            else:
                break

        return result, self.r.zscore(self.generate_free_key(version=version), result), version

    def pop_transaction(self, pipe: redis.client.Pipeline, version):
        free_key, lease_key = self.generate_free_key(version=version), self.generate_lease_key(version=version)
        [(member, score)] = pipe.zrevrange(free_key, 0, 0, withscores=True)
        member = member.decode()

        pipe.multi()
        pipe.zincrby(free_key, value=member, amount=-1)
        pipe.zincrby(lease_key, value=member, amount=1)
        return member

    def put(self, member, version):
        self.r.transaction(
            functools.partial(self.put_transaction, member=member, version=version),
            self.generate_free_key(version=version),
            self.generate_lease_key(version=version),
        )

    def put_transaction(self, pipe: redis.client.Pipeline, member, version):
        free_key, lease_key = self.generate_free_key(version=version), self.generate_lease_key(version=version)

        pipe.multi()
        pipe.zincrby(free_key, value=member, amount=1)
        pipe.zincrby(lease_key, value=member, amount=-1)

    def update(self, member_scores: dict):
        func = functools.partial(self.update_transaction, member_scores=member_scores)
        self.version = self.r.transaction(func, value_from_callable=True)

    def update_transaction(self, pipe: redis.client.Pipeline, member_scores):
        version = self.version + 1
        pipe.multi()
        pipe.zadd(self.generate_free_key(version=version), mapping=member_scores)
        return version

    def items(self):
        return [(k.decode(), v) for k, v in self.r.zrevrange(self.free_key, 0, -1, withscores=True)]

    def leased_items(self):
        return [(k.decode(), v) for k, v in self.r.zrevrange(self.lease_key, 0, -1, withscores=True)]

    def clear(self):
        self.r.delete(self.free_key, self.lease_key)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.pop()
        except ValueError:
            raise StopIteration

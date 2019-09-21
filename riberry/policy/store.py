import abc
from threading import local


class PolicyContextStore(abc.ABC):

    def get(self, item, default=None):
        raise NotImplementedError

    def set(self, item, value):
        raise NotImplementedError


class ThreadLocalPolicyContextStore(PolicyContextStore):
    _local = local()

    def get(self, item, default=None):
        return getattr(self._local, item, default)

    def set(self, item, value):
        setattr(self._local, item, value)

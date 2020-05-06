from contextlib import contextmanager

from .store import ThreadLocalPolicyContextStore, PolicyContextStore


class PolicyContext:
    store: PolicyContextStore = ThreadLocalPolicyContextStore()

    def __getitem__(self, item):
        return self.store.get(item, default=None)

    def __setitem__(self, item, value):
        self.store.set(item, value)

    @property
    def enabled(self):
        return self['enabled']

    @enabled.setter
    def enabled(self, value):
        self['enabled'] = value

    @property
    def subject(self):
        return self['subject']

    @subject.setter
    def subject(self, value):
        self['subject'] = value

    @property
    def environment(self):
        return self['environment']

    @environment.setter
    def environment(self, value):
        self['environment'] = value

    @contextmanager
    def scope(self, subject, environment):
        try:
            self.configure(subject=subject, environment=environment)
            yield
        finally:
            self.reset()

    @contextmanager
    def disabled_scope(self):
        _original_value = self.enabled
        try:
            self.enabled = False
            yield
        finally:
            self.enabled = _original_value

    def configure(
            self,
            subject,
            environment,
    ):
        self.enabled = True
        self.subject = subject
        self.environment = environment

    def reset(self):
        self.enabled = False
        self.subject = None
        self.environment = None

    @classmethod
    def current(cls):
        return cls()

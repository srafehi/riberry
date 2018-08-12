import abc
import functools
import inspect
from contextlib import contextmanager
from threading import local
from typing import Set, Union, Optional, Type, Callable

from riberry.exc import AuthorizationError


class NotApplicable(Exception):
    pass


class AuthorizationEngine:

    def __init__(self, name, *policy):
        self.name = name
        self.policies: Set[Union[PolicySet, Policy]] = set(policy)

    def authorize(self, context):
        results = []
        for policy_sey in self.policies:
            try:
                results.append(policy_sey.authorize(context=context))
            except NotApplicable:
                pass

        return False if False in results else True


class PolicyContext:

    _local = local()
    _no_default = object()

    def __getitem__(self, item):
        return getattr(self._local, item, None)

    def __setitem__(self, item, value):
        setattr(self._local, item, value)

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

    @property
    def engine(self) -> AuthorizationEngine:
        return self['policy_engine']

    @engine.setter
    def engine(self, value):
        self['policy_engine'] = value

    @property
    def on_deny(self) -> Optional[Exception]:
        return self['on_deny']

    @on_deny.setter
    def on_deny(self, value):
        self['on_deny'] = value

    @contextmanager
    def scope(self, subject, environment, policy_engine, on_deny: Optional[Union[Type, Callable]] = AuthorizationError):
        self.configure(subject=subject, environment=environment, policy_engine=policy_engine, on_deny=on_deny)
        yield
        self.reset()

    @contextmanager
    def disabled_scope(self):
        try:
            self.enabled = False
            yield
        finally:
            self.enabled = True

    def configure(self, subject, environment, policy_engine, on_deny: Optional[Union[Type, Callable]] = AuthorizationError):
        self.enabled = True
        self.subject = subject
        self.environment = environment
        self.engine = policy_engine
        self.on_deny = on_deny

    def reset(self):
        self.enabled = True
        self.subject = None
        self.environment = None
        self.engine = None
        self.on_deny = None

    @classmethod
    def current(cls):
        return cls()

    def authorize(self, resource, action, on_deny: Optional[Union[Type, Callable]] = _no_default):

        if not self.enabled:
            return True

        attr_context = AttributeContext(
            subject=self.subject,
            environment=self.environment,
            action=action,
            resource=resource
        )

        result = self.engine.authorize(attr_context)
        if result is False:
            on_deny = self.on_deny if on_deny is self._no_default else on_deny
            if inspect.isclass(on_deny) and issubclass(on_deny, Exception):
                raise on_deny
            elif callable(on_deny):
                on_deny(attr_context)
        return result

    def filter(self, resources, action):
        return [resource for resource in resources if self.authorize(resource, action, on_deny=None)]

    def post_filter(self, action):
        def outer(func):
            @functools.wraps(func)
            def inner(*args, **kwargs):
                result = func(*args, **kwargs)
                return self.filter(resources=result, action=action)
            return inner
        return outer

    def post_authorize(self, action, on_deny: Optional[Union[Type, Callable]] = _no_default):
        def outer(func):
            @functools.wraps(func)
            def inner(*args, **kwargs):
                result = func(*args, **kwargs)
                self.authorize(resource=result, action=action, on_deny=on_deny)
                return result
            return inner
        return outer


class AttributeContext:

    def __init__(self, subject, resource, action, environment):
        self.subject = subject
        self.resource = resource
        self.action = action
        self.environment = environment


class AuthorizationElement(metaclass=abc.ABCMeta):

    def target_clause(self, context: AttributeContext) -> bool:
        raise NotImplementedError

    def condition(self, context: AttributeContext) -> bool:
        raise NotImplementedError

    def apply(self, context: AttributeContext):
        if not self.target_clause(context=context):
            raise NotApplicable
        return self.condition(context=context)

    def authorize(self, context):
        raise NotImplementedError


class PolicyCollection(AuthorizationElement, metaclass=abc.ABCMeta):

    def __init__(self, *collection):
        self.collection: Set[AuthorizationElement] = set(collection)

    def authorize(self, context):
        if not self.apply(context=context):
            return False

        results = set()
        for policy in self.collection:
            try:
                results.add(policy.authorize(context=context))
            except NotApplicable:
                pass

        if any(results):
            return True
        return False if False in results else None


class PolicySet(PolicyCollection, metaclass=abc.ABCMeta):
    pass


class Policy(PolicyCollection, metaclass=abc.ABCMeta):
    pass


class Rule(AuthorizationElement, metaclass=abc.ABCMeta):

    def on_permit(self, context):
        pass

    def on_deny(self, context):
        pass

    def authorize(self, context):
        result = self.apply(context=context)
        self.on_permit(context=context) if result else self.on_deny(context=context)
        return result

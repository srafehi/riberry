

class BaseError(Exception):
    __msg__ = None
    __http_code__ = 500

    def __init__(self, target=None, data=None, **args):
        super(BaseError, self).__init__(self._fmt_message(**args, target=target))
        self.target = target
        self.exc_data = data or {}

    @classmethod
    def _fmt_message(cls, **args):
        return cls.__msg__.format(**args) if cls.__msg__ else f'{cls.__name__} was raised'

    def output(self):
        return {
            'code': type(self).__name__,
            'message': str(self),
            'target': self.target,
            'data': self.exc_data
        }


class AuthenticationError(BaseError):
    __msg__ = 'Wrong username or password supplied.'
    __http_code__ = 401

    def __init__(self):
        super(AuthenticationError, self).__init__(target='user')


class SessionExpired(BaseError):
    __msg__ = 'The user\'s session has expired.'
    __http_code__ = 401

    def __init__(self):
        super(SessionExpired, self).__init__(target='user')


class AuthorizationError(BaseError):
    __msg__ = 'User does not have access to the given resource.'
    __http_code__ = 403

    def __init__(self):
        super(AuthorizationError, self).__init__(target='user')


class ResourceNotFound(BaseError):
    __msg__ = 'The requested resource does not exist.'
    __http_code__ = 404

    def __init__(self, resource, identifier):
        super(ResourceNotFound, self).__init__(target=resource, data={
            'id': identifier
        })


class UnknownError(BaseError):
    __msg__ = 'An unknown error has occurred.'
    __http_code__ = 500

    def __init__(self, error=None):
        super(UnknownError, self).__init__()


class InputErrorGroup(BaseError):
    __msg__ = 'One or more errors occurred while validating the request.'
    __http_code__ = 400

    def __init__(self, *errors):
        super(InputErrorGroup, self).__init__(
            target=None,
            data={'errors': []}
        )
        self.extend(errors=errors)

    def extend(self, errors):
        self.exc_data['errors'] += [e.output() if isinstance(e, BaseError) else e for e in errors]


class RequiredInputError(BaseError):
    __msg__ = 'Required field {field!r} for {target} not provided.'
    __http_code__ = 400

    def __init__(self, target, field, internal_name=None):
        super(RequiredInputError, self).__init__(
            target=target,
            field=field,
            data=dict(internal=internal_name)
        )


class InvalidInputError(BaseError):
    __msg__ = 'Invalid {field!r} provided for {target}.'
    __http_code__ = 400

    def __init__(self, target, field, internal_name=None):
        super(InvalidInputError, self).__init__(
            target=target,
            field=field,
            data=dict(internal=internal_name)
        )


class InvalidEnumError(BaseError):
    __msg__ = 'Invalid {field!r} provided for {target}. Expected: {allowed_values}.'
    __http_code__ = 400

    def __init__(self, target, field, allowed_values, internal_name=None):
        super(InvalidEnumError, self).__init__(
            target=target,
            field=field,
            allowed_values=', '.join(repr(value) for value in allowed_values),
            data=dict(internal=internal_name)
        )


class UnknownInputError(BaseError):
    __msg__ = 'Unknown input field {field!r} provided for {target}.'
    __http_code__ = 400

    def __init__(self, target, field):
        super(UnknownInputError, self).__init__(target=target, field=field)


class UniqueInputConstraintError(BaseError):
    __msg__ = 'Cannot create {target} with {field}: {value!r}. This {field} is already in-use.'
    __http_code__ = 400

    def __init__(self, target, field, value):
        super(UniqueInputConstraintError, self).__init__(target=target, field=field, value=value)

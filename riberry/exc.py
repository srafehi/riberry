

class BaseError(Exception):
    __msg__ = None
    __http_code__ = 500

    def __init__(self, target=None, data=None, **args):
        super(BaseError, self).__init__(self._fmt_message(**args))
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

import base64
import hashlib
import hmac
import secrets

import riberry

TOKEN_LENGTH = 20


def make_token() -> str:
    """ Creates a cryptographically secure token. """

    return secrets.token_hex(nbytes=TOKEN_LENGTH)


def make_api_key(token: str, identifier: str, secret: str) -> str:
    """ Creates an API key with the given token. """

    prefix = f'{token}.{identifier}'
    key = f'{prefix}.{secret}'
    signature = hmac.new(key.encode(), prefix.encode(), hashlib.sha256).hexdigest().lower()
    prefix_encoded = base64.urlsafe_b64encode(prefix.encode()).decode()
    return f'{prefix_encoded}.{signature}'


def prefix_from_api_key(api_key: str) -> str:
    """ Extracts the prefix from the given API key. """

    prefix_encoded = api_key.split('.')[0]
    return base64.urlsafe_b64decode(prefix_encoded).decode()


def token_from_api_key(api_key: str) -> str:
    """ Extracts the token from the given API key. """

    return prefix_from_api_key(api_key).split('.')[0]


def identifier_from_api_key(api_key: str) -> str:
    """ Extracts the identifier from the given API key. """

    return prefix_from_api_key(api_key).split('.')[1]


def validate_api_key(api_key: str, secret: str):
    """ Validates the given API key's integrity.

     If the check fails, a riberry.exc.InvalidApiKeyError is raised.
    """

    # Re-generate the API key from the token and identifier encoded in the API key.
    # If any of these have been forged, the generated API key will not match the
    # provided API key.

    computed_api_key = make_api_key(
        token=token_from_api_key(api_key),
        identifier=identifier_from_api_key(api_key),
        secret=secret,
    )

    if api_key != computed_api_key:
        raise riberry.exc.InvalidApiKeyError

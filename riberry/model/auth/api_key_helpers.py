import base64
import hashlib
import hmac
import json
import secrets

import riberry

TOKEN_LENGTH = 18


def make_token() -> str:
    """ Creates a cryptographically secure token. """

    return secrets.token_hex(nbytes=TOKEN_LENGTH)


def make_api_key(payload: dict, secret: str) -> str:
    """ Creates an API key with the given token. """

    payload_encoded = encode_payload(payload=payload)
    key = f'{payload_encoded}.{secret}'
    signature = hmac.new(key.encode(), payload_encoded.encode(), hashlib.sha256).hexdigest().lower()
    return f'{payload_encoded}.{signature}'


def encode_payload(payload: dict) -> str:
    """ Serializes and encodes the given payload. """

    payload_json = json.dumps(payload, sort_keys=True)
    return base64.urlsafe_b64encode(payload_json.encode()).decode().replace('=', '_')


def decode_payload(payload_encoded: str) -> dict:
    """ Decodes and deserializes the given payload. """

    payload_json = base64.urlsafe_b64decode(payload_encoded.replace('_', '=')).decode()
    return json.loads(payload_json)


def encoded_payload_from_api_key(api_key: str) -> str:
    """ Returns the encoded payload from the given API key. """

    return api_key.split('.')[0]


def signature_from_api_key(api_key: str) -> str:
    """ Returns the hmac signature from the given API key. """

    return api_key.split('.')[-1]


def payload_from_api_key(api_key: str) -> dict:
    """ Returns the deserialized payload from the given API key. """

    return decode_payload(encoded_payload_from_api_key(api_key=api_key))


def verify_api_key(api_key: str, secret: str) -> dict:
    """ Validates the given API key's integrity.

    If the check fails, a riberry.exc.InvalidApiKeyError is raised.
    """

    # Re-generate the API key from the token and identifier encoded in the API key.
    # If any of these have been forged, the generated API key will not match the
    # provided API key.

    payload: dict = payload_from_api_key(api_key=api_key)
    computed_api_key = make_api_key(payload=payload, secret=secret)

    if api_key != computed_api_key:
        raise riberry.exc.InvalidApiKeyError

    return payload

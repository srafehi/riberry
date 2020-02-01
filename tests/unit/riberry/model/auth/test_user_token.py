from datetime import timedelta
from unittest import mock

import pendulum
import pytest

from riberry import model
from riberry.exc import InvalidApiKeyError
from riberry.model import auth


@pytest.fixture
def token_builder(dummy_user: auth.User):
    def _token_builder(type='test', token='TOKEN'):
        token = auth.UserToken(
            user_id=dummy_user.id,
            type=type,
            token=token,
            expires=None,
        )
        model.conn.add(token)
        model.conn.commit()
        return token

    return _token_builder


def test_not_expired(token_builder):
    user_token: auth.UserToken = token_builder()
    user_token.expires = pendulum.DateTime.utcnow() + timedelta(seconds=10)
    assert not user_token.expired()


def test_no_expiry(token_builder):
    user_token: auth.UserToken = token_builder()
    user_token.expires = None
    assert not user_token.expired()


def test_expired(token_builder):
    user_token: auth.UserToken = token_builder()
    user_token.expire()
    assert user_token.expired()


def test_api_key_invalid_modified_payload_token(token_builder):
    user_token: auth.UserToken = token_builder(token='T1')
    api_key = user_token.generate_api_key()
    payload = auth.api_key_helpers.payload_from_api_key(api_key)
    payload['token'] = 'T2'
    payload_encoded = auth.api_key_helpers.encode_payload(payload)
    api_key = f'{payload_encoded}.{auth.api_key_helpers.signature_from_api_key(api_key)}'

    with pytest.raises(InvalidApiKeyError):
        auth.UserToken.from_api_key(api_key)


def test_api_key_invalid_modified_payload_uid(token_builder):
    user_token: auth.UserToken = token_builder()
    api_key = user_token.generate_api_key()
    payload = auth.api_key_helpers.payload_from_api_key(api_key)
    payload['uid'] = payload['uid'] + 1
    payload_encoded = auth.api_key_helpers.encode_payload(payload)
    api_key = f'{payload_encoded}.{auth.api_key_helpers.signature_from_api_key(api_key)}'

    with pytest.raises(InvalidApiKeyError):
        auth.UserToken.from_api_key(api_key)


def test_api_key_invalid_modified_signature(token_builder):
    user_token: auth.UserToken = token_builder()
    api_key = user_token.generate_api_key()
    payload_encoded = auth.api_key_helpers.encoded_payload_from_api_key(api_key)
    api_key = f'{payload_encoded}.MODIFIED_SIGNATURE'

    with pytest.raises(InvalidApiKeyError):
        auth.UserToken.from_api_key(api_key)


def test_api_key_invalid_changed_token(token_builder):
    user_token: auth.UserToken = token_builder(token='T1')
    api_key = user_token.generate_api_key()
    user_token.token = 'T2'

    with pytest.raises(InvalidApiKeyError):
        auth.UserToken.from_api_key(api_key)


def test_api_key_invalid_changed_user(token_builder):
    user_token: auth.UserToken = token_builder()
    api_key = user_token.generate_api_key()
    user_token.user_id = user_token.user.id = user_token.user.id + 1

    with pytest.raises(InvalidApiKeyError):
        auth.UserToken.from_api_key(api_key)


def test_api_key_invalid_expired(token_builder):
    user_token: auth.UserToken = token_builder()
    user_token.expires = pendulum.DateTime.utcnow() - timedelta(seconds=10)
    api_key = user_token.generate_api_key()

    with pytest.raises(InvalidApiKeyError):
        auth.UserToken.from_api_key(api_key)


def test_api_key_invalid_changed_secret(token_builder):
    user_token: auth.UserToken = token_builder()
    api_key = user_token.generate_api_key()
    with mock.patch.object(auth.UserToken, '_UserToken__secret', return_value='CHANGED_SECRET'):
        with pytest.raises(InvalidApiKeyError):
            auth.UserToken.from_api_key(api_key)


def test_api_key_valid(token_builder):
    user_token: auth.UserToken = token_builder()
    api_key = user_token.generate_api_key()
    assert auth.UserToken.from_api_key(api_key) == user_token


def test_expire_api_key_exc(token_builder):
    user_token: auth.UserToken = token_builder()
    api_key = user_token.generate_api_key()
    auth.UserToken.expire_api_key(api_key=api_key)

    with pytest.raises(InvalidApiKeyError):
        auth.UserToken.from_api_key(api_key)


def test_expire_api_key_assertion(token_builder):
    user_token: auth.UserToken = token_builder()
    api_key = user_token.generate_api_key()
    auth.UserToken.expire_api_key(api_key=api_key)

    assert user_token.expired()

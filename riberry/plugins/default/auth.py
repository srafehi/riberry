import hashlib
import os

from riberry.model import auth, config


def make_hash(text) -> bytes:
    hash = hashlib.sha512()
    hash.update(text)
    return hash.hexdigest().encode()


def salted_hash(plain: bytes, salt: bytes):
    plain_hash: bytes = make_hash(plain)
    return make_hash(plain_hash + salt)


def check_password(input_password: bytes, hashed_password: bytes):
    size = len(hashed_password) // 2
    password_hash, salt = hashed_password[:size], hashed_password[size:]
    input_hash = salted_hash(input_password, salt) + salt
    return hashed_password == input_hash


def hash_password(password: bytes) -> bytes:
    salt = make_hash(os.urandom(1024))
    return salted_hash(password, salt) + salt


class DummyAuthenticationProvider:

    def authenticate(self, username, password):
        user = auth.User.query().filter_by(username=username).first()
        return check_password(input_password=(password or '').encode(), hashed_password=user.password.encode())


config.config.authentication_provider = DummyAuthenticationProvider()

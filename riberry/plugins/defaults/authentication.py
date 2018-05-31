import hashlib
import os

from riberry.model import auth
from riberry.plugins.interfaces import AuthenticationProvider


def make_hash(text) -> bytes:
    hash_ = hashlib.sha512()
    hash_.update(text)
    return hash_.hexdigest().encode()


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


class DefaultAuthenticationProvider(AuthenticationProvider):

    @classmethod
    def name(cls) -> str:
        return 'default'

    def secure_password(self, password: str) -> bytes:
        return hash_password(password=(password or '').encode())

    def authenticate(self, username: str, password: str) -> bool:
        user = auth.User.query().filter_by(username=username).first()
        if user:
            return check_password(input_password=(password or '').encode(), hashed_password=user.password.encode())
        else:
            return False

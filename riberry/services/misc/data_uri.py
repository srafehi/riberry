import base64
import mimetypes
from email.message import Message
from functools import lru_cache
from typing import Dict, Optional, Any
from urllib.parse import parse_qsl
from urllib.request import urlopen


class DataUri:
    """ Parses a data-uri string allowing you to access its contents and properties """

    def __init__(self, data_uri: str):
        with self._open_data_uri(data_uri) as f:
            self.properties: Dict[str, str] = self._extract_properties(f.headers)
            self.binary: bytes = f.read()
            self.size: int = len(self.binary)

    @property
    def text(self) -> str:
        """ Returns the data-uri's data as a string. """

        return self.binary.decode() if self.binary is not None else None

    @classmethod
    def is_data_uri(cls, value: str) -> bool:
        """ Checks to see if the given string is a data-uri object. """

        if not value.startswith('data:'):
            return False

        # noinspection PyBroadException
        try:
            return bool(cls(value))
        except:
            return False

    @staticmethod
    def _extract_properties(headers: Message) -> Dict[str, str]:
        """ Extracts properties stored within the data-uri. """

        properties_string = headers['content-type'].split(';', 1)[-1]
        properties_items = parse_qsl(properties_string)
        return dict(properties_items)

    @staticmethod
    @lru_cache(maxsize=2)
    def _open_data_uri(data_uri):
        """ Creates and returns a file-like object for the data-uri. """

        return urlopen(data_uri)


class DataUriBuilder:
    """ Assists in the creation of data-uri strings. Accepts strings, bytes or file/file-like objects. """

    fallback_file_type = 'application/octet-stream'

    def __init__(self, content: Optional[Any], filename: str = None):
        self.content = content
        self.filename = filename
        self._content_encoded = None

    @property
    def content_encoded(self) -> bytes:
        """ Returns the content encoded as a base64 byte string. """

        if self.content and self._content_encoded is None:
            try:
                content = self.content.read()
            except AttributeError:
                if isinstance(self.content, str):
                    content = self.content.encode()
                elif isinstance(self.content, bytes):
                    content = self.content
                else:
                    raise ValueError(
                        f'DataUriBuilder.content is unsupported type ({type(self.content)}). '
                        f'Expected str, bytes or file/file-like object.'
                    )
            self._content_encoded = base64.b64encode(content).decode()
        return self._content_encoded or b''

    @property
    def mimetype(self):
        """ Returns the current mimetype based on the supplied filename. """

        file_type, _ = mimetypes.guess_type(self.filename)
        return file_type or self.fallback_file_type

    def build(self) -> str:
        """ Constructs the data-uri and returns the result. """

        properties = f'name={self.filename};' if self.filename else ''
        return f'data:{self.mimetype};{properties}base64,{self.content_encoded}'

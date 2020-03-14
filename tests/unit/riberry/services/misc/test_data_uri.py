import io
import json
import tempfile
from pathlib import Path

import pytest

from riberry.services.misc.data_uri import DataUri, DataUriBuilder


class TestDataUri:

    @staticmethod
    @pytest.mark.parametrize('data_uri_string,binary', [
        ('data:text/plain;charset=utf-8;base64,SGVsbG8gV29ybGQ=', b'Hello World'),
        ('data:text/plain;charset=utf-8;base64,', b''),
        ('data:text/plain;charset=utf-8;base64,MTIzNDU=', b'12345'),
        ('data:text/plain;charset=utf-8;,Hello%20World', b'Hello World'),
        ('data:text/plain;charset=utf-8;,', b''),
        ('data:text/plain;charset=utf-8;,12345', b'12345'),
    ])
    def test_data_uri_binary(data_uri_string, binary):
        assert DataUri(data_uri=data_uri_string).binary == binary

    @staticmethod
    def test_data_uri_no_properties():
        assert 'charset' in DataUri('data:,').properties

    @staticmethod
    @pytest.mark.parametrize('data_uri_string,properties', [
        ('data:;name=request.json,', {'name': 'request.json'}),
        ('data:;charset=utf-8;name=request.json,', {'charset': 'utf-8', 'name': 'request.json'}),
        ('data:application/json;charset=utf-8;name=request.json,', {'charset': 'utf-8', 'name': 'request.json'}),
    ])
    def test_data_uri_properties(data_uri_string, properties):
        assert DataUri(data_uri_string).properties == properties

    @staticmethod
    @pytest.mark.parametrize('data_uri_string,size', [
        ('data:text/plain;charset=utf-8;base64,SGVsbG8gV29ybGQ=', 11),
        ('data:text/plain;charset=utf-8;base64,', 0),
        ('data:text/plain;charset=utf-8;base64,MTIzNDU=', 5),
    ])
    def test_data_uri_size(data_uri_string, size):
        assert DataUri(data_uri=data_uri_string).size == size

    @staticmethod
    @pytest.mark.parametrize('data_uri_string,text', [
        ('data:text/plain;charset=utf-8;base64,SGVsbG8gV29ybGQ=', 'Hello World'),
        ('data:text/plain;charset=utf-8;base64,', ''),
        ('data:text/plain;charset=utf-8;base64,MTIzNDU=', '12345'),
        ('data:text/plain;charset=utf-8;,Hello%20World', 'Hello World'),
        ('data:text/plain;charset=utf-8;,', ''),
        ('data:text/plain;charset=utf-8;,12345', '12345'),
    ])
    def test_data_uri_text(data_uri_string, text):
        assert DataUri(data_uri=data_uri_string).text == text

    @staticmethod
    @pytest.mark.parametrize('data_uri_string,validity', [
        ('data:,', True),
        ('data:;base64,', True),
        ('data:text/plain,', True),
        ('data:text/plain;base64,', True),
        ('data:;charset=utf-8,', True),
        ('data:;charset=utf-8;base64,', True),
        ('data:text/plain;charset=utf-8;base64,', True),
        ('data:text/plain;charset=utf-8;name=hello-world;base64,MTIzNDU=', True),
        ('data:text/plain;charset=utf-8;name=hello-world;,Hello%20World', True),
        ('data:text/plain;base64,MTIzNDU=', True),
        ('data:text/plain;base64,', True),
        ('data:text/plain;base64', False),
        ('text/plain;charset=utf-8;base64,', False),
    ])
    def test_data_uri_valid(data_uri_string, validity):
        assert DataUri.is_data_uri(data_uri_string) == validity


class TestDataUriBuilder:

    def test_for_serializable_content(self):
        assert DataUriBuilder.for_serializable({"Hello": "World"}).content == '{"Hello": "World"}'

    def test_for_serializable_filename(self):
        assert DataUriBuilder.for_serializable({"Hello": "World"}, filename='filename').filename == 'filename'

    def test_for_serializable_json_kwargs(self):
        assert DataUriBuilder.for_serializable({"Hello": "World"}, indent=1).content == '{\n "Hello": "World"\n}'

    @staticmethod
    @pytest.mark.parametrize('content,filename,data_uri', [
        (None, None, 'data:application/octet-stream;base64,'),
        ('', None, 'data:application/octet-stream;base64,'),
        (b'', None, 'data:application/octet-stream;base64,'),
        ('', 'file', 'data:application/octet-stream;name=file;base64,'),
        ('', 'file.json', 'data:application/json;name=file.json;base64,'),
        ('{}', 'file.json', 'data:application/json;name=file.json;base64,e30='),
    ])
    def test_build(content, filename, data_uri):
        assert DataUriBuilder(content=content, filename=filename).build() == data_uri

    @staticmethod
    @pytest.mark.parametrize('content,content_encoded', [
        ('', ''),
        (b'', ''),
        ('12345', 'MTIzNDU='),
        (b'12345', 'MTIzNDU='),
    ])
    def test_content_encoded_file(content, content_encoded):
        with tempfile.TemporaryDirectory() as directory:
            mode = 'b' if isinstance(content, bytes) else ''
            file_path = Path(directory) / 'temp.file'
            with open(file_path, f'w{mode}') as fw:
                fw.write(content)

            with open(file_path, f'r{mode}') as fr:
                assert DataUriBuilder(fr).content_encoded == content_encoded

    @staticmethod
    @pytest.mark.parametrize('content', [
        123,
        123.4,
        True,
        [],
        {},
        object(),
    ])
    def test_content_encoded_invalid(content):
        instance = DataUriBuilder(content=content)
        with pytest.raises(ValueError):
            _ = instance.content_encoded

    @staticmethod
    @pytest.mark.parametrize('content,content_encoded', [
        ('', ''),
        (b'', ''),
        ('12345', 'MTIzNDU='),
        (b'12345', 'MTIzNDU='),
    ])
    def test_content_encoded_io(content, content_encoded):
        io_instance = io.BytesIO() if isinstance(content, bytes) else io.StringIO()
        io_instance.write(content)
        io_instance.seek(0)
        assert DataUriBuilder(io_instance).content_encoded == content_encoded

    @staticmethod
    @pytest.mark.parametrize('content,content_encoded', [
        (None, ''),
        ('', ''),
        (b'', ''),
        ('12345', 'MTIzNDU='),
        (b'12345', 'MTIzNDU='),
    ])
    def test_content_encoded_string(content, content_encoded):
        assert DataUriBuilder(content).content_encoded == content_encoded

    @staticmethod
    @pytest.mark.parametrize('filename,mimetype', [
        ('file.json', 'application/json'),
        ('file.xml', 'application/xml'),
        ('file.png', 'image/png'),
        ('file', DataUriBuilder.fallback_file_type),
        ('', DataUriBuilder.fallback_file_type),
        (None, DataUriBuilder.fallback_file_type),
    ])
    def test_mimetype(filename, mimetype):
        assert DataUriBuilder('', filename=filename).mimetype == mimetype

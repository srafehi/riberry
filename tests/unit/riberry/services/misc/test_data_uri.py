import pytest

from riberry.services.misc.data_uri import DataUri


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

    @staticmethod
    @pytest.mark.parametrize('data_uri_string,size', [
        ('data:text/plain;charset=utf-8;base64,SGVsbG8gV29ybGQ=', 11),
        ('data:text/plain;charset=utf-8;base64,', 0),
        ('data:text/plain;charset=utf-8;base64,MTIzNDU=', 5),
    ])
    def test_data_uri_size(data_uri_string, size):
        assert DataUri(data_uri=data_uri_string).size == size

    @staticmethod
    @pytest.mark.parametrize('data_uri_string,properties', [
        ('data:;name=request.json,', {'name': 'request.json'}),
        ('data:;charset=utf-8;name=request.json,', {'charset': 'utf-8', 'name': 'request.json'}),
        ('data:application/json;charset=utf-8;name=request.json,', {'charset': 'utf-8', 'name': 'request.json'}),
    ])
    def test_data_uri_properties(data_uri_string, properties):
        assert DataUri(data_uri_string).properties == properties

    @staticmethod
    def test_data_uri_no_properties():
        assert 'charset' in DataUri('data:,').properties

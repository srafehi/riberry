import pytest
from sqlalchemy import Column, Integer, String, LargeBinary, Text
from sqlalchemy.ext.declarative import declarative_base

from riberry.model import helpers


Base = declarative_base()


class DummyModel(Base):
    __tablename__ = 'dummy'

    id = Column(Integer, primary_key=True)
    string_field_no_length = Column(String())
    string_field_with_length = Column(String(5))
    binary_field_no_length = Column(LargeBinary())
    binary_field_with_length = Column(LargeBinary(10))
    text_field_no_length = Column(Text())
    text_field_with_length = Column(Text(15))


@pytest.mark.parametrize(['attribute', 'length'], [
    (DummyModel.string_field_no_length, None),
    (DummyModel.string_field_with_length, 5),
    (DummyModel.binary_field_no_length, None),
    (DummyModel.binary_field_with_length, 10),
    (DummyModel.text_field_no_length, None),
    (DummyModel.text_field_with_length, 15),
])
def test_max_string_length(attribute, length):
    assert helpers.max_string_length(attribute) == length


@pytest.mark.parametrize(['attribute', 'value', 'expected_value'], [
    (DummyModel.string_field_no_length, 'ABCDEFG', 'ABCDEFG'),
    (DummyModel.string_field_with_length, 'ABCDEFG', 'ABCDE'),
    (DummyModel.binary_field_no_length, b'ABCDEFGHIJKL', b'ABCDEFGHIJKL'),
    (DummyModel.binary_field_with_length, b'ABCDEFGHIJKL', b'ABCDEFGHIJ'),
    (DummyModel.text_field_no_length, 'ABCDEFGHIJKLMNOPQ', 'ABCDEFGHIJKLMNOPQ'),
    (DummyModel.text_field_with_length, 'ABCDEFGHIJKLMNOPQ', 'ABCDEFGHIJKLMNO'),
])
def test_trim_attribute_value(attribute, value, expected_value):
    assert helpers.trim_attribute_value(attribute, value) == expected_value

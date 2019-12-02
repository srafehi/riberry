import json

import pytest

from riberry.exc import InputErrorGroup
from riberry.model.interface import InputValueDefinition, InputValueEnum
from riberry.services.job import verify_inputs


def _dummy_input_value(
        name='Input',
        internal_name='input',
        type='text',
        required=False,
        default=None,
        enums=None
):
    return InputValueDefinition(
        name=name,
        internal_name=internal_name,
        type=type,
        required=required,
        default_binary=json.dumps(default).encode() if default else None,
        allowed_value_enumerations=[
            InputValueEnum(value=json.dumps(enum).encode())
            for enum in enums or []
        ]
    )


def _run_verify_inputs(
        input_value_definitions=None,
        input_file_definitions=None,
        input_values=None,
        input_files=None
):
    return verify_inputs(
        input_value_definitions=input_value_definitions or [],
        input_file_definitions=input_file_definitions or [],
        input_values=input_values or {},
        input_files=input_files or {},
    )


def test_input_value_optional_no_input_given():
    input_value = _dummy_input_value(required=False)
    result = _run_verify_inputs(
        input_value_definitions=[input_value],
    )
    assert result == ({input_value: None}, {})


def test_input_value_optional_has_default_no_input_given():
    input_value = _dummy_input_value(required=False, default='value')
    result = _run_verify_inputs(
        input_value_definitions=[input_value],
    )
    assert result == ({input_value: 'value'}, {})


def test_input_value_optional_has_default_has_enum_valid_input_given():
    input_value = _dummy_input_value(required=False, default='value', enums=['value'])
    result = _run_verify_inputs(
        input_value_definitions=[input_value],
        input_values={'input': 'value'},
    )

    assert result == ({input_value: 'value'}, {})


def test_input_value_optional_has_default_has_enum_invalid_input_given():
    with pytest.raises(InputErrorGroup):
        _run_verify_inputs(
            input_value_definitions=[
                _dummy_input_value(required=False, default='value', enums=['value']),
            ],
            input_values={'input': 'invalid'},
        )


def test_input_value_optional_input_given():
    input_value = _dummy_input_value(required=False, default='value')
    result = _run_verify_inputs(
        input_value_definitions=[input_value],
        input_values={'input': 'valid'}
    )
    assert result == ({input_value: 'valid'}, {})


def test_input_value_required_no_input_given():
    with pytest.raises(InputErrorGroup):
        _run_verify_inputs(
            input_value_definitions=[
                _dummy_input_value(required=True),
            ],
        )


def test_input_value_required_has_default_no_input_given():
    input_value = _dummy_input_value(required=True, default='value')
    result = _run_verify_inputs(
        input_value_definitions=[input_value],
    )

    assert result == ({input_value: 'value'}, {})


def test_input_value_required_has_default_has_enum_no_input_given():
    input_value = _dummy_input_value(required=True, default='value', enums=['value'])
    result = _run_verify_inputs(
        input_value_definitions=[input_value],
    )

    assert result == ({input_value: 'value'}, {})


def test_input_value_required_has_default_has_enum_valid_input_given():
    input_value = _dummy_input_value(required=True, default='value', enums=['value'])
    result = _run_verify_inputs(
        input_value_definitions=[input_value],
        input_values={'input': 'value'},
    )

    assert result == ({input_value: 'value'}, {})


def test_input_value_required_has_default_has_enum_invalid_input_given():
    with pytest.raises(InputErrorGroup):
        _run_verify_inputs(
            input_value_definitions=[
                _dummy_input_value(required=True, default='value', enums=['value']),
            ],
            input_values={'input': 'invalid'},
        )


def test_input_value_required_input_given():
    input_value = _dummy_input_value(required=True, default='value')
    result = _run_verify_inputs(
        input_value_definitions=[input_value],
        input_values={'input': 'valid'},
    )

    assert result == ({input_value: 'valid'}, {})

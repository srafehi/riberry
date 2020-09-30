import pytest

import riberry
from riberry.model.interface import InputDefinition, InputValueInstance, InputFileInstance
from riberry.services.job import JobBuilder


@pytest.fixture(scope='module')
def create_test_form():
    with riberry.model.conn:
        application = riberry.model.application.Application(
            name='application',
            internal_name='application',
            type='type',
        )
        riberry.model.conn.add(application)
        riberry.model.conn.flush()

        instance = riberry.model.application.ApplicationInstance(
            name='instance',
            internal_name='instance',
            application_id=application.id,
        )
        riberry.model.conn.add(instance)
        riberry.model.conn.flush()

        form = riberry.model.interface.Form(
            name='form',
            internal_name='form',
            instance_id=instance.id,
            application_id=application.id,
        )
        riberry.model.conn.add(form)
        riberry.model.conn.commit()

    yield

    with riberry.model.conn:
        application = riberry.model.application.Application.query().filter_by(internal_name='application').one()
        riberry.model.conn.delete(application)
        riberry.model.conn.commit()


@pytest.fixture
def form(create_test_form):
    return riberry.model.interface.Form.query().filter_by(internal_name='form').one()


class TestJobBuilder:

    @staticmethod
    def test_load_form_internal_name(form):
        assert JobBuilder.load_form('form') == form

    @staticmethod
    def test_load_form_instance(form):
        assert JobBuilder.load_form(form) == form

    @staticmethod
    def test_load_owner_username(dummy_user):
        assert JobBuilder.load_owner('johndoe') == dummy_user

    @staticmethod
    def test_load_owner_instance(dummy_user):
        assert JobBuilder.load_owner(dummy_user) == dummy_user

    @staticmethod
    def test_load_owner_subject(dummy_user):
        with riberry.services.policy.policy_scope(dummy_user):
            assert JobBuilder.load_owner(None) == dummy_user

    @staticmethod
    def test_load_owner_none():
        with pytest.raises(riberry.exc.GenericValidationError):
            JobBuilder.load_owner(None)

    @staticmethod
    @pytest.mark.parametrize('definition,input_data,expected_result', [

        # No definition
        (None, None, ([], [])),

        # Definition with a schema defining a string
        (
                {'schema': {'type': 'string'}, 'options': {}},
                'value',
                (
                        [
                            InputValueInstance(
                                name='definition',
                                internal_name=JobBuilder.input_value_instance_key,
                                value='value',
                            )
                        ],
                        []
                )
        ),

        # Definition with a schema defining a string and an input defining a file
        (
                {'schema': {'type': 'string'}, 'options': {}},
                'data:application/json;name=file.json,',
                (
                        [
                            InputValueInstance(
                                name='definition',
                                internal_name=JobBuilder.input_value_instance_key,
                                value=f"riberry://model/InputFileInstance?"
                                      f"internal_name={JobBuilder.input_value_instance_key}",
                            )
                        ],
                        [
                            InputFileInstance(
                                name=JobBuilder.input_value_instance_key,
                                internal_name=JobBuilder.input_value_instance_key,
                                filename='file.json',
                                size=0,
                                binary=b'',
                            )
                        ]
                )
        ),

        # Definition with a schema defining an object to be flattened
        (
                {
                    'schema': {
                        'type': 'object',
                        'required': ['p1', 'p2'],
                        'properties': {
                            'p1': {'type': 'string'},
                            'p2': {'type': 'integer'},
                        }
                    },
                    'options': {
                        'flatten': True,
                    }
                },
                {'p1': 'hello', 'p2': 123},
                (
                        [
                            InputValueInstance(name='p1', internal_name='p1', value='hello'),
                            InputValueInstance(name='p2', internal_name='p2', value=123),
                        ],
                        [

                        ]
                )
        ),

        # Definition with a schema defining an object to be flattened + an input file
        (
                {
                    'schema': {
                        'type': 'object',
                        'required': ['p1', 'p2'],
                        'properties': {
                            'p1': {'type': 'string'},
                            'p2': {'type': 'integer'},
                        }
                    },
                    'options': {
                        'flatten': True,
                    }
                },
                {'p1': 'data:application/json;name=file.json,', 'p2': 123},
                (
                        [
                            InputValueInstance(name='p1', internal_name='p1',
                                               value='riberry://model/InputFileInstance?internal_name=p1'),
                            InputValueInstance(name='p2', internal_name='p2', value=123),
                        ],
                        [
                            InputFileInstance(
                                name='p1',
                                internal_name='p1',
                                filename='file.json',
                                size=0,
                                binary=b'',
                            )
                        ]
                )
        ),

        # Definition with a schema defining an object
        (
                {
                    'schema': {
                        'type': 'object',
                        'required': ['p1', 'p2'],
                        'properties': {
                            'p1': {'type': 'string'},
                            'p2': {'type': 'integer'},
                        }
                    },
                    'options': {}
                },
                {'p1': 'hello', 'p2': 123},
                (
                        [
                            InputValueInstance(
                                name='definition',
                                internal_name=JobBuilder.input_value_instance_key,
                                value={"p1": "hello", "p2": 123},
                            ),
                        ],
                        [

                        ]
                )
        ),

        # Definition with a schema defining a object + an input file
        (
                {
                    'schema': {
                        'type': 'object',
                        'required': ['p1', 'p2'],
                        'properties': {
                            'p1': {'type': 'string'},
                            'p2': {'type': 'integer'},
                        }
                    },
                    'options': {}
                },
                {'p1': 'data:application/json;name=file.json,', 'p2': 123},
                (
                        [
                            InputValueInstance(
                                name='definition',
                                internal_name=JobBuilder.input_value_instance_key,
                                value={"p1": "riberry://model/InputFileInstance?internal_name=p1", "p2": 123},
                            ),
                        ],
                        [
                            InputFileInstance(
                                name='p1',
                                internal_name='p1',
                                filename='file.json',
                                size=0,
                                binary=b'',
                            )
                        ]
                )
        ),

    ])
    def test_build_input_instances(model_to_dict, definition, input_data, expected_result):
        if definition is None:
            input_definition = None
        else:
            input_definition = InputDefinition(name='definition')
            input_definition.definition = definition

        expected_values, expected_files = expected_result
        expected_values = list(map(model_to_dict, expected_values))
        expected_files = list(map(model_to_dict, expected_files))

        actual_values, actual_files = JobBuilder.build_input_instances(input_definition, input_data)
        actual_values = list(map(model_to_dict, actual_values))
        actual_files = list(map(model_to_dict, actual_files))

        assert (actual_values, actual_files) == (expected_values, expected_files)

import pytest
from collections import namedtuple

from riberry.model.interface import InputFileInstance
from riberry.services.job.file_extractor import InputFileExtractor


FileExtractorTest = namedtuple('FileExtractorTest', ['input_data', 'root', 'result'])


class TestInputFileExtractor:

    @staticmethod
    @pytest.mark.parametrize('scenario', [
        # Basic inputs with no files
        FileExtractorTest(input_data={}, root=None, result=({}, [])),
        FileExtractorTest(input_data=[], root='root', result=([], [])),
        FileExtractorTest(input_data='', root='root', result=('', [])),
        FileExtractorTest(input_data='string', root='root', result=('string', [])),
        FileExtractorTest(input_data=None, root=None, result=(None, [])),
        FileExtractorTest(input_data=-1, root=None, result=(-1, [])),
        FileExtractorTest(input_data=0, root=None, result=(0, [])),
        FileExtractorTest(input_data=1000, root=None, result=(1000, [])),
        FileExtractorTest(input_data=12.3, root=None, result=(12.3, [])),
        FileExtractorTest(input_data=True, root=None, result=(True, [])),

        # Dicts and lists inputs with no files
        FileExtractorTest(
            input_data={'key': 'value'},
            root=None,
            result=({'key': 'value'}, [])
        ),
        FileExtractorTest(
            input_data={'key': [{'key': [1, {}, []]}]},
            root=None,
            result=({'key': [{'key': [1, {}, []]}]}, [])
        ),
        FileExtractorTest(
            input_data=[1, '2', [{}]],
            root='root',
            result=([1, '2', [{}]], [])
        ),

        # Inputs with files
        FileExtractorTest(
            input_data='data:,',
            root='root',
            result=(
                    'riberry://model/InputFileInstance?internal_name=root',
                    [
                        InputFileInstance(internal_name='root', name='root', filename='root', size=0, binary=b'')
                    ]
            )
        ),
        FileExtractorTest(
            input_data=['data:,'],
            root='root',
            result=(
                    ['riberry://model/InputFileInstance?internal_name=root.0'],
                    [
                        InputFileInstance(internal_name='root.0', name='root.0', filename='root.0', size=0, binary=b'')
                    ]
            )
        ),
        FileExtractorTest(
            input_data=[{'key': 'data:,'}],
            root='root',
            result=(
                    [{'key': 'riberry://model/InputFileInstance?internal_name=root.0.key'}],
                    [
                        InputFileInstance(
                            internal_name='root.0.key', name='root.0.key', filename='root.0.key', size=0, binary=b'')
                    ]
            )
        ),
        FileExtractorTest(
            input_data={'key': 'data:,'},
            root=None,
            result=(
                    {'key': 'riberry://model/InputFileInstance?internal_name=key'},
                    [
                        InputFileInstance(internal_name='key', name='key', filename='key', size=0, binary=b'')
                    ]
            )
        ),
        FileExtractorTest(
            input_data={
                'a': [
                    {'b': 'data:,', 'c': [0, 'data:,']}
                ],
                'b': 'data:,'
            },
            root=None,
            result=(
                    {
                        'a': [
                            {
                                'b': 'riberry://model/InputFileInstance?internal_name=a.0.b',
                                'c': [0, 'riberry://model/InputFileInstance?internal_name=a.0.c.1']
                            }
                        ],
                        'b': 'riberry://model/InputFileInstance?internal_name=b'
                    },
                    [
                        InputFileInstance(internal_name='a.0.b', name='a.0.b', filename='a.0.b', size=0, binary=b''),
                        InputFileInstance(
                            internal_name='a.0.c.1', name='a.0.c.1', filename='a.0.c.1', size=0, binary=b''),
                        InputFileInstance(internal_name='b', name='b', filename='b', size=0, binary=b''),
                    ]
            )
        ),
        FileExtractorTest(
            input_data={'key': 'data:application/json;name=file.json;base64,e30='},
            root=None,
            result=(
                    {'key': 'riberry://model/InputFileInstance?internal_name=key'},
                    [
                        InputFileInstance(internal_name='key', name='key', filename='file.json', size=2, binary=b'{}'),
                    ]
            )
        ),
    ])
    def test_extract(scenario: FileExtractorTest):
        extractor = InputFileExtractor(input_data=scenario.input_data)
        actual_input_data, actual_file_instances = extractor.extract(root=scenario.root)

        actual_file_instances = [dict(
            name=instance.name,
            internal_name=instance.internal_name,
            filename=instance.filename,
            size=instance.size,
            binary=instance.binary
        ) for instance in actual_file_instances]

        expected_input_data = scenario.result[0]
        expected_file_instances = [dict(
            name=instance.name,
            internal_name=instance.internal_name,
            filename=instance.filename,
            size=instance.size,
            binary=instance.binary
        ) for instance in scenario.result[-1]]

        assert (actual_input_data, actual_file_instances) == (expected_input_data, expected_file_instances)

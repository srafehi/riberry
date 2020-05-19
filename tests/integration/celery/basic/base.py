import json

import pytest

from riberry import testing


class BasicBaseTestSuite(testing.base.TestApplication):

    entry_point_step_name = None

    def test_execution_successful(self):
        testing.helpers.verify_execution_completion(
            execution=self.execution,
            completion_status='SUCCESS',
            process_events=True,
        )

    def test_expected_streams(self):
        testing.comparators.compare_streams(
            execution=self.execution,
            expected={
                'Stream Entry': {
                    self.entry_point_step_name: 1,
                    'save_all': 1,
                },
                'Stream #0': {
                    'process': 1,
                    'save': 1,
                },
                'Stream #1': {
                    'process': 1,
                    'save': 1,
                }
            }
        )

    def test_artifacts_created(self):
        testing.comparators.compare_artifacts(
            execution=self.execution,
            expected={
                '0.txt': {
                    'name': '0.txt',
                    'type': 'output',
                    'category': 'Default',
                },
                '1.txt': {
                    'name': '1.txt',
                    'type': 'output',
                    'category': 'Default',
                },
                'all.txt': {
                    'name': 'all.txt',
                    'type': 'output',
                    'category': 'Default',
                }
            }
        )

    def test_job_input_files(self):
        assert not self.job_files

    def test_output_file_contents(self):
        artifact_all = None
        for artifact in self.execution.artifacts:
            if artifact.filename == 'all.txt':
                artifact_all = artifact
                break
        else:
            pytest.fail('Could not find `all.txt` artifact')

        output = json.loads(artifact_all.binary.binary)
        assert output == [
            {'number': 0, 'output': 0},
            {'number': 1, 'output': 1},
        ]

    def test_job_input_values(self):
        raise NotImplementedError

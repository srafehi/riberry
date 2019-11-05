from collections import defaultdict
from typing import Callable, Tuple, Dict, Any, Iterable

import riberry

ArtifactProcessor = Callable[[riberry.model.job.JobExecutionArtifact], Tuple[str, dict]]


def _compare(expected: Any, actual: Any, failure_message: str):
    """ Simples equality assertion function.

    Useful when testing with pytest which will log the input parameters
    in case of a failure.
    """
    assert expected == actual, failure_message


def compare_streams(
        execution: riberry.model.job.JobExecution,
        expected: Dict[str, Dict[str, int]]
):
    """ Compares the given streams with the execution's actual streams.

     Example of `expected` parameter:

     {
        "STREAM_NAME_1": {     # stream name
            "STEP_NAME_1": 1,  # key: step name, value: step count,
            "STEP_NAME_2": 2   # key: step name, value: step count,
        },
        "STREAM_NAME_2": {     # stream name
            "STEP_NAME_1": 1   # key: step name, value: step count,
        }
    }
    """

    actual = get_actual_stream_step_counts(execution)
    _compare(expected, actual, 'Streams do not match')


def compare_artifacts(
        execution: riberry.model.job.JobExecution,
        expected: dict,
        artifact_processor: ArtifactProcessor = None
):
    """ Compares the given streams with the execution's actual streams.

    Example of default `expected` parameter:

    {
        "ARTIFACT_1_FILENAME": {
            "ARTIFACT_NAME": ...,
            "ARTIFACT_TYPE": ...,
            "ARTIFACT_CATEGORY": ...
        },
        "ARTIFACT_2_FILENAME": {
            ...
        }
    }
    """

    artifact_processor = artifact_processor or default_artifact_processor
    actual = {key: value for key, value in get_actual_artifacts(execution, artifact_processor)}
    _compare(expected, actual, 'Artifacts do not match')


def get_actual_stream_step_counts(execution: riberry.model.job.JobExecution) -> Dict[str, Dict[str, int]]:
    """ Returns all streams and the count tallies for steps under those streams. """

    stream_counts_actual = defaultdict(lambda: defaultdict(int))
    for stream in execution.streams:
        for step in stream.steps:
            stream_counts_actual[stream.name][step.name] += 1

    return {
        stream_name: dict(steps)
        for stream_name, steps in stream_counts_actual.items()
    }


def default_artifact_processor(artifact: riberry.model.job.JobExecutionArtifact) -> Tuple[str, dict]:
    """ Returns basic information for generated artifacts """

    return artifact.filename, {
        'name': artifact.name,
        'type': artifact.type.value,
        'category': artifact.category,
    }


def get_actual_artifacts(
        execution: riberry.model.job.JobExecution,
        artifact_processor: ArtifactProcessor
) -> Iterable[Tuple[str, dict]]:
    """ Returns an iterable of all artifacts for the given execution. """

    return (artifact_processor(artifact) for artifact in execution.artifacts)

import json
import sys
import traceback
from typing import Union, Optional

import riberry
from riberry.exc import BaseError
from riberry.model.job import ArtifactType
from .. import current_context as context
from ..util.events import create_event


def create_artifact(filename: str, content: Union[bytes, str], name: str = None,
                    type: Union[str, ArtifactType] = ArtifactType.output, category='Default', data: dict = None,
                    stream: str = None, task_id: str = None, root_id: str = None):
    task_id = task_id or context.current.task_id
    root_id = root_id or context.current.root_id
    stream = stream or context.current.stream

    if name is None:
        name = filename

    if content is not None and not isinstance(content, (bytes, str)):
        content = json.dumps(content)

    if isinstance(content, str):
        content = content.encode()

    if isinstance(type, ArtifactType):
        type = type.value

    try:
        ArtifactType(type)
    except ValueError as exc:
        raise ValueError(f'ArtifactType enum has no value {type!r}.'
                         f'Supported types: {", ".join(ArtifactType.__members__)}') from exc

    create_event(
        'artifact',
        root_id=root_id,
        task_id=task_id,
        data={
            'name': str(name),
            'type': str(type),
            'category': str(category),
            'data': data if isinstance(data, dict) else {},
            'stream': str(stream) if stream else None,
            'filename': str(filename),
        },
        binary=content
    )


def create_artifact_from_traceback(
        name: Optional[str] = None,
        filename: Optional[str] = None,
        category: str = 'Intercepted',
        type: riberry.model.job.ArtifactType = riberry.model.job.ArtifactType.error,
):
    exc_type, exc, tb = sys.exc_info()

    if not exc_type:
        return

    if isinstance(exc, BaseError):
        error_content = f'{traceback.format_exc()}\n\n{"-" * 32}\n\n{json.dumps(exc.output(), indent=2)}'.encode()
    else:
        error_content = traceback.format_exc().encode()

    task = context.current.task
    create_artifact(
        name=name if name else f'Exception {task.name}',
        type=type,
        category=category,
        data={
            'Error Type': exc.__class__.__name__,
            'Error Message': str(exc),
        },
        filename=filename if filename else f'{task.name}-{task.request.id}.log',
        content=error_content
    )

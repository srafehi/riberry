from typing import Union, Optional

import riberry
from riberry.model.job import ArtifactType


class Artifact:

    @staticmethod
    def create(
            filename: str,
            content: Union[bytes, str],
            name: str = None,
            type: Union[str, ArtifactType] = ArtifactType.output,
            category='Default',
            data: dict = None,
    ):
        return riberry.app.actions.artifacts.create_artifact(
            filename=filename,
            content=content,
            name=name,
            type=type,
            category=category,
            data=data,
        )

    @staticmethod
    def create_from_traceback(
            name: Optional[str] = None,
            filename: Optional[str] = None,
            category: str = 'Intercepted',
            type: riberry.model.job.ArtifactType = riberry.model.job.ArtifactType.error,
    ):
        return riberry.app.actions.artifacts.create_artifact_from_traceback(
            name=name,
            filename=filename,
            type=type,
            category=category,
        )

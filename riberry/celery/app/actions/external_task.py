import json
from typing import Optional
import uuid

import riberry


def create_external_task(
        job_execution: riberry.model.job.JobExecution,
        name: str = None,
        type: str = 'external',
        external_task_id: Optional[str] = None,
        input_data: Optional[bytes] = None
):
    if external_task_id is None:
        external_task_id = str(uuid.uuid4())

    if name is None:
        name = external_task_id

    if isinstance(input_data, str):
        input_data = input_data.encode()

    if input_data is not None and not isinstance(input_data, bytes):
        input_data = json.dumps(input_data).encode()

    external_task = riberry.model.job.JobExecutionExternalTask(
        job_execution=job_execution,
        stream_id=None,
        task_id=external_task_id,
        name=name,
        type=type,
        input_data=input_data,
    )

    riberry.model.conn.add(external_task)
    riberry.model.conn.commit()

    return external_task


def mark_as_ready(external_task_id, output_data):
    task: riberry.model.job.JobExecutionExternalTask = riberry.model.job.JobExecutionExternalTask.query().filter_by(
        task_id=external_task_id,
    ).one()

    if isinstance(output_data, str):
        output_data = output_data.encode()

    if output_data is not None and not isinstance(output_data, bytes):
        output_data = json.dumps(output_data).encode()

    task.status = 'READY'
    task.output_data = output_data
    riberry.model.conn.commit()

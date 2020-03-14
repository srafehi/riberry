from typing import Union, Any, Optional

import riberry
from ..env import current_context


def create_job(
        form: Union[riberry.model.interface.Form, str],
        job_name: Optional[str] = None,
        input_data: Any = None,
        execute_on_creation: bool = True,
        owner: Optional[Union[riberry.model.auth.User, str]] = None,
):
    job_execution = current_context.current.job_execution
    owner = owner if owner else job_execution.creator if job_execution else riberry.policy.context.subject

    if job_execution:
        job_name = job_name or f'Via {job_execution.job.name} / #{job_execution.id}'

    with riberry.services.policy.policy_scope(user=owner):
        job = riberry.services.job.create_job(
            form=form,
            job_name=job_name,
            input_data=input_data,
            execute_on_creation=execute_on_creation,
            parent_execution=job_execution,
        )

        riberry.model.conn.commit()
        return job

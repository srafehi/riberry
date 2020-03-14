from typing import Union, Optional, Any

from riberry import model, policy
from .job_builder import JobBuilder


def jobs_by_form_id(form_id):
    return model.job.Job.query().filter_by(form_id=form_id).all()


def create_job(
        form: Union[model.interface.Form, str],
        job_name: Optional[str] = None,
        input_data: Any = None,
        execute_on_creation: bool = True,
        parent_execution: Optional[model.job.JobExecution] = None,
):
    return JobBuilder(
        form=form,
        job_name=job_name,
        input_data=input_data,
        execute_on_creation=execute_on_creation,
        parent_execution=parent_execution,
    ).build()


@policy.context.post_authorize(action='view')
def job_by_id(job_id):
    return model.job.Job.query().filter_by(id=job_id).one()


@policy.context.post_filter(action='view')
def job_executions_by_id(job_id):
    return model.job.JobExecution.query().filter_by(job_id=job_id).all()


def create_job_execution_by_job_id(job_id):
    job = job_by_id(job_id=job_id)
    return create_job_execution(job=job)


def create_job_execution(job, parent_execution=None):
    execution = model.job.JobExecution(job=job, creator=policy.context.subject, parent_execution=parent_execution)

    policy.context.authorize(execution, action='create')
    model.conn.add(execution)

    return execution


def input_file_instance_by_id(input_file_instance_id) -> model.interface.InputFileInstance:
    return model.interface.InputFileInstance.query().filter_by(id=input_file_instance_id).one()


def delete_job_by_id(job_id):
    delete_job(job=job_by_id(job_id=job_id))


@policy.context.post_authorize(action='view')
def delete_job(job):
    model.conn.delete(job)

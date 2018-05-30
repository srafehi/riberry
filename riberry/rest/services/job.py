from riberry import services
from riberry.rest import view_models


def create_job(form_id, name, input_values, input_files, execute):
    job = services.job.create_job(
        form_id=form_id,
        name=name,
        input_values=input_values,
        input_files=input_files,
        execute=execute
    )
    return view_models.Job(model=job, options={'expand': {'executions': {}}}).to_dict()


def jobs_by_form_id(form_id, options=None):
    jobs = services.job.jobs_by_form_id(form_id=form_id)
    return [view_models.Job(model=job, options=options).to_dict() for job in jobs]


def job_by_id(job_id, options):
    job = services.job.job_by_id(job_id=job_id)
    return view_models.Job(model=job, options=options).to_dict()


def job_executions_by_id(job_id, options):
    job_executions = services.job.job_executions_by_id(job_id=job_id)
    return [view_models.JobExecution(model=execution, options=options).to_dict() for execution in job_executions]


def create_job_execution(job_id):
    execution = services.job.create_job_execution_by_job_id(job_id=job_id)
    return view_models.JobExecution(model=execution, options=None).to_dict()


def summary_overall():
    return services.job.summary_overall()

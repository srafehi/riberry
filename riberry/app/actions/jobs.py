import riberry
from ..env import current_context


def create_job(form_name, job_name=None, input_values=None, input_files=None, owner=None, execute=True):
    form: riberry.model.interface.Form = riberry.model.interface.Form.query().filter_by(
        internal_name=form_name,
    ).first()

    job_execution = current_context.current.job_execution
    job_execution_user = owner if owner else job_execution.creator

    with riberry.services.policy.policy_scope(user=job_execution_user):
        job = riberry.services.job.create_job(
            form_id=form.id,
            name=job_name or f'Via {job_execution.job.name} / #{job_execution.id}',
            input_values=input_values or {},
            input_files=input_files or {},
            execute=execute,
            parent_execution=job_execution,
        )

        riberry.model.conn.commit()
        return job

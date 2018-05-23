from riberry import services
from riberry.rest import view_models


def create_job(instance_interface_id, name, input_values, input_files):
    job = services.job.create_job(
        instance_interface_id=instance_interface_id,
        name=name,
        input_values=input_values,
        input_files=input_files
    )
    return view_models.Job(model=job, options=None).to_dict()


def jobs_by_instance_interface_id(instance_interface_id, options=None):
    jobs = services.job.jobs_by_instance_interface_id(instance_interface_id=instance_interface_id)
    return [view_models.Job(model=job, options=options).to_dict() for job in jobs]

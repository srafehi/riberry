import uuid
from typing import Union, Callable, Any

import riberry
from riberry import testing


class JobSpecification:
    """ Specification for a job to be created. """

    def __init__(
            self,
            form_internal_name: str,
            username: str,
            job_prefix: str = 'TEST_',
            job_name: Union[Callable[[], str], str] = uuid.uuid4,
            input_data: dict = None,
            execute: bool = True,
    ):
        self.form_internal_name = form_internal_name
        self.username = username
        self.input_data = input_data
        self.job_prefix = job_prefix
        self.job_name = job_name
        self.execute = execute

    def create(self) -> riberry.model.job.Job:
        return testing.helpers.create_job(
            form_internal_name=self.form_internal_name,
            username=self.username,
            job_prefix=self.job_prefix,
            job_name=self.job_name,
            input_data=self.input_data,
            execute=self.execute,
        )


class TestApplication:
    """ Helper class used to test job executions.

    Call `setup_test_scenario` when setting up the class to populate the
    default class values.
    """

    job_id: int = None
    execution_id: int = None
    job_values: dict = None
    job_files: dict = None

    @property
    def execution(self) -> riberry.model.job.JobExecution:
        return self.__execution()

    @property
    def job(self) -> riberry.model.job.Job:
        return self.__job()

    @property
    def job_data(self) -> Any:
        input_value_instance_key = riberry.services.job.JobBuilder.input_value_instance_key
        return self.job_values.get(input_value_instance_key) if self.job_values else None

    @classmethod
    def delete_test_job(cls):
        riberry.model.conn.delete(cls.__job())
        riberry.model.conn.commit()

    @classmethod
    def setup_test_scenario(cls, job_specification: JobSpecification):
        testing.helpers.setup_test_scenario(test_app=cls, job_specification=job_specification)

    @classmethod
    def __execution(cls) -> riberry.model.job.JobExecution:
        return riberry.model.job.JobExecution.query().filter_by(id=cls.execution_id).one()

    @classmethod
    def __job(cls):
        return riberry.model.job.Job.query().filter_by(id=cls.job_id).one()

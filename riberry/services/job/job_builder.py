""" Classes to build new jobs. """

from enum import Enum
from typing import Union, Optional, List, Dict, Tuple, Any

import jsonschema

import riberry
from .file_extractor import InputFileExtractor


class JobValidationType(Enum):
    """ Types of validations for jobs. """

    validate_job_name = 'validate_job_name'
    validate_input_data = 'validate_input_data'


class JobBuilder:
    """ Creates a new job for the given form and input data. """

    def __init__(
            self,
            form: Union[riberry.model.interface.Form, str],
            job_name: str,
            input_data: Any = None,
            execute_on_creation: bool = False,
            parent_execution: Optional[riberry.model.job.JobExecution] = None,
            skip_validations: Optional[List[JobValidationType]] = None,
            owner: Optional[Union[riberry.model.auth.User, str]] = None,
    ):
        self.form: riberry.model.interface.Form = self.load_form(form=form)
        self.job_name: str = job_name
        self.input_data: Any = input_data
        self.execute_on_creation: bool = execute_on_creation
        self.parent_execution: Optional[riberry.model.job.JobExecution] = parent_execution
        self.validator = JobBuilderValidator(self, skip_validations=skip_validations)
        self.owner: Optional[Union[riberry.model.auth.User, str]] = self.load_owner(owner)

    @property
    def input_definition(self) -> Optional[riberry.model.interface.InputDefinition]:
        """ Returns the input definition for the given form. """

        input_definitions: List[riberry.model.interface.InputDefinition] = self.form.input_definitions
        if input_definitions:
            assert len(input_definitions) == 1, 'Forms with 2 or more InputDefinitions are not yet supported.'
            return input_definitions[0]

    def build(self) -> riberry.model.job.Job:
        """ Builds and returns a new job. """

        self.validate(raise_on_errors=True)
        input_values, input_files = self.build_input_instances(self.input_definition, self.input_data)
        job = riberry.model.job.Job(
            form=self.form,
            name=self.job_name,
            files=input_files,
            values=input_values,
            creator=self.owner,
        )

        riberry.policy.context.authorize(job, action='create')
        if self.execute_on_creation:
            execution = riberry.model.job.JobExecution(
                job=job,
                creator=self.owner,
                parent_execution=self.parent_execution,
            )

            riberry.policy.context.authorize(execution, action='create')
            riberry.model.conn.add(execution)

        riberry.model.conn.add(job)
        return job

    def validate(self, raise_on_errors: bool = False) -> bool:
        """ Validates the created job. """

        return self.validator.validate(raise_on_errors=raise_on_errors)

    @staticmethod
    def build_input_instances(
            input_definition: riberry.model.interface.InputDefinition,
            input_data: Any,
    ) -> Tuple[
        List[riberry.model.interface.InputValueInstance],
        List[riberry.model.interface.InputFileInstance],
    ]:
        """ Returns the created input instances for the given definition and data. """

        # No input definition to process
        if not input_definition:
            return [], []

        # Return input value if the input data is not a dictionary
        if not isinstance(input_data, dict):
            input_files = []
            input_data = InputFileExtractor.extract_from_value(
                input_data=input_data,
                files=input_files,
                parent=input_definition.internal_name,
            )
            input_value = riberry.model.interface.InputValueInstance(
                name=input_definition.name,
                internal_name=input_definition.internal_name,
            )
            input_value.value = input_data
            return [input_value], input_files

        # Flatten the input dictionary into input values and files
        elif input_definition.definition['options'].get('flatten'):
            # extract any files
            extractor = InputFileExtractor(input_data=input_data)
            input_data, input_files = extractor.extract()

            # flatten input_data
            input_values = []
            for key, value in input_data.items():
                input_value_instance = riberry.model.interface.InputValueInstance(
                    name=key,
                    internal_name=key,
                )
                input_value_instance.value = value
                input_values.append(input_value_instance)

            return input_values, input_files

        # Return the input data as an input value instance alongside any extract files
        else:
            extractor = InputFileExtractor(input_data=input_data)
            input_data, input_files = extractor.extract()
            input_value = riberry.model.interface.InputValueInstance(
                name=input_definition.name,
                internal_name=input_definition.internal_name,
            )
            input_value.value = input_data
            return [input_value], input_files

    @staticmethod
    def load_form(form: Union[riberry.model.interface.Form, str]) -> riberry.model.interface.Form:
        """ Resolves a form instance from the given input. """

        if isinstance(form, riberry.model.interface.Form):
            return form
        else:
            return riberry.model.interface.Form.query().filter_by(internal_name=form).one()

    @staticmethod
    def load_owner(owner: Optional[Union[riberry.model.auth.User, str]]) -> riberry.model.auth.User:
        """ Resolves a user instance from the given input. """

        if isinstance(owner, riberry.model.auth.User):
            return owner
        elif isinstance(owner, str):
            return riberry.model.auth.User.query().filter_by(username=owner).one()
        elif riberry.policy.context.subject:
            return riberry.policy.context.subject
        else:
            raise riberry.exc.GenericValidationError('No user provided to JobBuilder.owner')


class JobBuilderValidator:
    """ Validates the attributes provided to the JobBuilder. """

    def __init__(
            self,
            job_builder: JobBuilder,
            skip_validations: Optional[List[JobValidationType]] = None,
    ):
        self.job_builder: JobBuilder = job_builder
        self.skip_validations: List[JobValidationType] = skip_validations or []
        self._errors: Optional[Dict[JobValidationType, List[riberry.exc.BaseError]]] = None

    @property
    def errors(self) -> List[riberry.exc.BaseError]:
        """ Returns errors associated with the JobBuilder. """

        if self._errors is None:
            self.validate()
        return list(error for error_list in self._errors.values() for error in error_list)

    def validate(self, raise_on_errors: bool = False) -> bool:
        """ Validates the JobBuilder instance. """

        self._errors = {}
        self.validate_job_name(raise_on_errors=False)
        self.validate_input_data(raise_on_errors=False)
        return self._check_errors(raise_on_errors, self.errors)

    def validate_input_data(self, raise_on_errors: bool = False) -> bool:
        """ Validates the JobBuilder's input data against its input definition. """

        if self._should_skip(JobValidationType.validate_input_data):
            return True

        if not self.job_builder.input_definition:
            if self.job_builder.input_data:
                return self._process_errors(
                    JobValidationType.validate_input_data,
                    raise_on_errors,
                    errors=[
                        riberry.exc.GenericValidationError(message='Input provided to form which does not expect any.')
                    ]
                )
            return True

        validator = jsonschema.Draft7Validator(self.job_builder.input_definition.definition['schema'])
        errors = [
            riberry.exc.GenericValidationError(message=err.message)
            for err in validator.iter_errors(self.job_builder.input_data)
        ]

        return self._process_errors(JobValidationType.validate_job_name, raise_on_errors, errors)

    def validate_job_name(self, raise_on_errors: bool = False) -> bool:
        """ Validates the JobBuilder's job name. """

        errors: List[riberry.exc.BaseError] = []
        if not self.job_builder.job_name:
            err = riberry.exc.RequiredInputError(target='job', field='name')
            errors.append(err)
        else:
            if riberry.model.job.Job.query().filter_by(name=self.job_builder.job_name).first():
                err = riberry.exc.UniqueInputConstraintError(target='job', field='name',
                                                             value=self.job_builder.job_name)
                errors.append(err)

        return self._process_errors(JobValidationType.validate_job_name, raise_on_errors, errors)

    def _process_errors(
            self,
            validation_type: JobValidationType,
            raise_on_error: bool, errors: List[riberry.exc.BaseError],
    ):
        """ Stores the errors and raises them on request. """

        if errors:
            self._errors[validation_type] = list(errors)
            self._check_errors(raise_on_error=raise_on_error, errors=errors)
            return True
        elif validation_type in self._errors:
            del self._errors[validation_type]
        return False

    def _should_skip(self, validation_type: JobValidationType) -> bool:
        """ Returns True if the validation should be skipped. """

        return validation_type in self.skip_validations

    @staticmethod
    def _check_errors(raise_on_error: bool, errors: List[riberry.exc.BaseError]) -> bool:
        """ Checks to see if errors exist, and optionally raises them if so. """

        if errors and raise_on_error:
            if len(errors) == 1:
                raise errors[0]
            else:
                raise riberry.exc.ErrorGroup(*errors)
        return bool(errors)

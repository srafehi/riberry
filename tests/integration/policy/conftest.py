import pytest

import riberry
from riberry.model.application import Application, ApplicationInstance, Heartbeat, ApplicationInstanceSchedule
from riberry.model.auth import User
from riberry.model.group import Group, ResourceGroupAssociation, GroupPermission
from riberry.model.interface import Form, InputDefinition, InputValueDefinition, InputValueEnum, InputFileDefinition
from riberry.model.job import Job, JobExecution
from riberry.model.misc import ResourceType


@pytest.fixture
def associate():
    def _associate(group, resource):

        if isinstance(resource, str):
            group.permissions.append(GroupPermission(name=resource))
            return

        resource_type = (
            ResourceType.user if isinstance(resource, User) else
            ResourceType.form if isinstance(resource, Form) else
            ResourceType.application if isinstance(resource, Application) else
            None
        )
        group.resource_associations.append(ResourceGroupAssociation(
            resource_id=resource.id,
            resource_type=resource_type,
        ))

    return _associate


@pytest.fixture
def create_user():
    def _create(username):
        instance = User(
            username=username,
            password=User.secure_password('password'),
            details=riberry.model.auth.UserDetails(),
        )
        riberry.model.conn.add(instance)
        riberry.model.conn.flush()
        return instance

    return _create


@pytest.fixture
def create_group():
    def _create(name, permissions=()):
        instance = Group(
            name=name,
            permissions=[
                riberry.model.group.GroupPermission(name=permission)
                for permission in permissions
            ]
        )
        riberry.model.conn.add(instance)
        riberry.model.conn.flush()
        return instance

    return _create


@pytest.fixture
def create_application():
    def _create(name):
        instance = Application(
            name=name,
            internal_name=name,
            type='test',
            instances=[
                ApplicationInstance(
                    name=name,
                    internal_name=name,
                    heartbeat=Heartbeat(),
                    schedules=[
                        ApplicationInstanceSchedule(parameter='active'),
                        ApplicationInstanceSchedule(parameter='custom'),
                    ]
                ),
            ]
        )
        riberry.model.conn.add(instance)
        riberry.model.conn.flush()
        return instance

    return _create


@pytest.fixture
def create_form(create_application):
    def _create(name, application_instance=None):
        if not application_instance:
            application = create_application(f'app.{name}')
            application_instance = application.instances[0]
        instance = Form(
            name=name,
            internal_name=name,
            application=application_instance.application,
            instance=application_instance,
            input_definition=InputDefinition(
                name='input_definition',
                definition_string='{}',
            ),
            input_value_definitions=[
                InputValueDefinition(
                    name='input_value_definition',
                    internal_name='input_value_definition',
                    type='text',
                    allowed_value_enumerations=[InputValueEnum(value=b'enum')],
                )
            ],
            input_file_definitions=[
                InputFileDefinition(
                    name='input_file_definition',
                    internal_name='input_file_definition',
                    type='csv',
                )
            ]
        )
        riberry.model.conn.add(instance)
        riberry.model.conn.flush()
        return instance

    return _create


@pytest.fixture
def create_job():
    def _create(name, form, creator):
        instance = Job(name=name, form=form, creator=creator)
        riberry.model.conn.add(instance)
        riberry.model.conn.flush()
        return instance
    return _create


@pytest.fixture
def create_execution():
    def _create(job, creator=None):
        instance = JobExecution(job=job, creator=creator or job.creator)
        riberry.model.conn.add(instance)
        riberry.model.conn.flush()
        return instance
    return _create

import pytest

import riberry
from riberry.model.application import Application, ApplicationInstance, Heartbeat, ApplicationInstanceSchedule
from riberry.model.auth import User
from riberry.model.group import Group
from riberry.model.interface import Form, InputDefinition
from riberry.model.job import Job, JobExecution, JobSchedule, JobExecutionStream, JobExecutionStreamStep
from riberry.policy.permissions import FormDomain, SystemDomain, ApplicationDomain


@pytest.fixture
def scenario_single_group_user_form(empty_database, create_user, create_group, create_form):
    group: Group = create_group('policy_group', permissions=[])
    user: User = create_user('policy_user')
    form: Form = create_form('policy_form')
    yield group, user, form


@pytest.fixture
def scenario_single_form_domain_unassociated(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    associate(group, FormDomain.PERM_ACCESS)
    associate(group, user)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_single_form_domain_associated(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    associate(group, FormDomain.PERM_ACCESS)
    associate(group, user)
    associate(group, form)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_single_sys_domain(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    associate(group, SystemDomain.PERM_ACCESS)
    associate(group, user)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_single_app_domain_unassociated(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    associate(group, ApplicationDomain.PERM_ACCESS)
    associate(group, user)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_single_app_domain_associated(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    associate(group, ApplicationDomain.PERM_ACCESS)
    associate(group, user)
    associate(group, form.application)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_multiple_group_user_form(empty_database, create_user, create_group, create_form):
    group1: Group = create_group('policy_group1', permissions=[])
    group2: Group = create_group('policy_group2', permissions=[])
    user1: User = create_user('policy_user1')
    user2: User = create_user('policy_user2')
    form1: Form = create_form('policy_form1')
    form2: Form = create_form('policy_form2')
    yield (group1, group2), (user1, user2), (form1, form2)


def _query_all(model_type, *_):
    return model_type.query().all()


def _query_get(model_type, id_):
    return model_type.query().get(id_)


def _query_filter_by(model_type, id_):
    return model_type.query().filter_by(id=id_).first()


def _query_filter(model_type, id_):
    return model_type.query().filter(model_type.id == id_).first()


def _schedule_param_id_getter(param):
    return lambda f: next(s.id for s in f.instance.schedules if s.parameter == param)


COMMON_TESTS = [
    (Form, lambda f: f.id, _query_all),
    (Form, lambda f: f.id, _query_get),
    (Form, lambda f: f.id, _query_filter_by),
    (Form, lambda f: f.id, _query_filter),
    (InputDefinition, None, _query_all),
    (InputDefinition, lambda f: f.input_definition.id, _query_get),
    (InputDefinition, lambda f: f.input_definition.id, _query_filter_by),
    (InputDefinition, lambda f: f.input_definition.id, _query_filter),
    (ApplicationInstance, lambda f: f.instance_id, _query_all),
    (ApplicationInstance, lambda f: f.instance_id, _query_get),
    (ApplicationInstance, lambda f: f.instance_id, _query_filter_by),
    (ApplicationInstance, lambda f: f.instance_id, _query_filter),
    (Application, lambda f: f.application_id, _query_all),
    (Application, lambda f: f.application_id, _query_get),
    (Application, lambda f: f.application_id, _query_filter_by),
    (Application, lambda f: f.application_id, _query_filter),
    (Heartbeat, None, _query_all),
    (Heartbeat, lambda f: f.instance.heartbeat.id, _query_get),
    (Heartbeat, lambda f: f.instance.heartbeat.id, _query_filter_by),
    (Heartbeat, lambda f: f.instance.heartbeat.id, _query_filter),
]


APPLICATION_INSTANCE_SCHEDULE_TESTS = [
    (ApplicationInstanceSchedule, None, _query_all),
    (ApplicationInstanceSchedule, _schedule_param_id_getter('active'), _query_all),
    (ApplicationInstanceSchedule, _schedule_param_id_getter('active'), _query_filter_by),
    (ApplicationInstanceSchedule, _schedule_param_id_getter('active'), _query_filter),
    (ApplicationInstanceSchedule, _schedule_param_id_getter('custom'), _query_all),
    (ApplicationInstanceSchedule, _schedule_param_id_getter('custom'), _query_filter_by),
    (ApplicationInstanceSchedule, _schedule_param_id_getter('custom'), _query_filter),
]


@pytest.mark.parametrize(['model_type', 'get_id', 'query_func'], [
    *COMMON_TESTS,
    *APPLICATION_INSTANCE_SCHEDULE_TESTS,
])
def test_form_domain_user_has_no_access(scenario_single_form_domain_unassociated, model_type, get_id, query_func):
    group, user, form = scenario_single_form_domain_unassociated
    id_ = get_id(form) if callable(get_id) else None
    with riberry.services.policy.policy_scope(user):
        assert not query_func(model_type, id_)


@pytest.mark.parametrize(['model_type', 'get_id', 'query_func'], [
    *COMMON_TESTS
])
def test_form_domain_user_has_access(scenario_single_form_domain_associated, model_type, get_id, query_func):
    group, user, form = scenario_single_form_domain_associated
    id_ = get_id(form) if callable(get_id) else None
    with riberry.services.policy.policy_scope(user):
        assert query_func(model_type, id_)


@pytest.mark.parametrize(['model_type', 'get_id', 'query_func'], [
    *COMMON_TESTS,
    *APPLICATION_INSTANCE_SCHEDULE_TESTS,
])
def test_sys_domain_user_has_access(scenario_single_sys_domain, model_type, get_id, query_func):
    group, user, form = scenario_single_sys_domain
    id_ = get_id(form) if callable(get_id) else None
    with riberry.services.policy.policy_scope(user):
        assert query_func(model_type, id_)


@pytest.mark.parametrize(['model_type', 'get_id', 'query_func'], [
    *COMMON_TESTS,
    *APPLICATION_INSTANCE_SCHEDULE_TESTS,
])
def test_app_domain_user_has_no_access(scenario_single_app_domain_unassociated, model_type, get_id, query_func):
    group, user, form = scenario_single_app_domain_unassociated
    id_ = get_id(form) if callable(get_id) else None
    with riberry.services.policy.policy_scope(user):
        assert not query_func(model_type, id_)


@pytest.mark.parametrize(['model_type', 'get_id', 'query_func'], [
    *[t for t in COMMON_TESTS if t[0] in (Application, ApplicationInstance, Heartbeat)],
    *APPLICATION_INSTANCE_SCHEDULE_TESTS,
])
def test_app_domain_user_has_access(scenario_single_app_domain_associated, model_type, get_id, query_func):
    group, user, form = scenario_single_app_domain_associated
    id_ = get_id(form) if callable(get_id) else None
    with riberry.services.policy.policy_scope(user):
        assert query_func(model_type, id_)


def test_form_domain_user_with_no_access_to_instance_schedules(scenario_single_form_domain_associated):
    group, user, form = scenario_single_form_domain_associated
    with riberry.services.policy.policy_scope(user):
        assert not _query_all(ApplicationInstanceSchedule)


def test_form_domain_user_with_access_to_builtin_instance_schedules(scenario_single_form_domain_associated, associate):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_APP_SCHEDULES_READ_BUILTIN)
    with riberry.services.policy.policy_scope(user):
        assert [sched.parameter for sched in _query_all(ApplicationInstanceSchedule)] == ['active']


def test_form_domain_user_with_access_to_all_instance_schedules(scenario_single_form_domain_associated, associate):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_APP_SCHEDULES_READ)
    with riberry.services.policy.policy_scope(user):
        assert [sched.parameter for sched in _query_all(ApplicationInstanceSchedule)] == ['active', 'custom']


def test_form_domain_user_with_no_access_to_create_job(scenario_single_form_domain_associated, associate):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    form_id = form.id
    with riberry.services.policy.policy_scope(user):
        with pytest.raises(riberry.exc.AuthorizationError):
            job = Job(name='job', form_id=form_id, creator=user)
            riberry.model.conn.add(job)
            riberry.model.conn.commit()


@pytest.mark.parametrize(['permission'], [
    [FormDomain.PERM_JOB_CREATE],
    [FormDomain.PERM_JOB_CREATE_SELF],
])
def test_form_domain_user_with_access_to_create_job(scenario_single_form_domain_associated, associate, permission):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, permission)

    with riberry.services.policy.policy_scope(user):
        job = Job(name='job', form_id=form.id, creator=user)
        riberry.model.conn.add(job)
        riberry.model.conn.commit()
        assert job.id


@pytest.mark.parametrize(['permission'], [
    [FormDomain.PERM_JOB_READ],
    [FormDomain.PERM_JOB_READ_SELF],
])
def test_form_domain_user_with_access_to_read_own_job(scenario_single_form_domain_associated, associate, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    associate(group, permission)

    job = create_job(name='job', form=form, creator=user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        assert Job.query().get(job_id)


def test_form_domain_user_with_no_access_to_read_other_job(scenario_single_form_domain_associated, associate, create_user, create_job):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ_SELF)

    job = create_job(name='job', form=form, creator=other_user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        assert not Job.query().get(job_id)


def test_form_domain_user_with_access_to_read_other_job(scenario_single_form_domain_associated, associate, create_user, create_job):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)

    job = create_job(name='job', form=form, creator=other_user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        assert Job.query().get(job_id)


def test_form_domain_user_with_no_access_to_update_own_job(scenario_single_form_domain_associated, associate, create_job):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)

    job = create_job(name='job', form=form, creator=user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        job = Job.query().get(job_id)
        with pytest.raises(riberry.exc.AuthorizationError):
            job.name = 'new_name'
            riberry.model.conn.add(job)
            riberry.model.conn.commit()


@pytest.mark.parametrize(['permission'], [
    [FormDomain.PERM_JOB_UPDATE_SELF],
    [FormDomain.PERM_JOB_UPDATE],
])
def test_form_domain_user_with_access_to_update_own_job(scenario_single_form_domain_associated, associate, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, permission)

    job = create_job(name='job', form=form, creator=user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        job = Job.query().get(job_id)
        job.name = 'new_name'
        riberry.model.conn.add(job)
        riberry.model.conn.commit()


@pytest.mark.parametrize(['permission'], [
    [None],
    [FormDomain.PERM_JOB_UPDATE_SELF],
])
def test_form_domain_user_with_no_access_to_update_other_job(scenario_single_form_domain_associated, associate, create_user, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)
    if permission:
        associate(group, permission)

    job = create_job(name='job', form=form, creator=other_user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        job = Job.query().get(job_id)
        with pytest.raises(riberry.exc.AuthorizationError):
            job.name = 'new_name'
            riberry.model.conn.add(job)
            riberry.model.conn.commit()


def test_form_domain_user_with_access_to_update_other_job(scenario_single_form_domain_associated, associate, create_user, create_job):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_UPDATE)

    job = create_job(name='job', form=form, creator=other_user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        job = Job.query().get(job_id)
        job.name = 'new_name'
        riberry.model.conn.add(job)
        riberry.model.conn.commit()
        riberry.model.conn.expire(job)
        assert job.name == 'new_name'


def test_form_domain_user_with_no_access_to_delete_own_job(scenario_single_form_domain_associated, associate, create_job):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)

    job = create_job(name='job', form=form, creator=user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        job = Job.query().get(job_id)
        with pytest.raises(riberry.exc.AuthorizationError):
            riberry.model.conn.delete(job)
            riberry.model.conn.flush()


@pytest.mark.parametrize(['permission'], [
    [FormDomain.PERM_JOB_DELETE_SELF],
    [FormDomain.PERM_JOB_DELETE],
])
def test_form_domain_user_with_access_to_delete_own_job(scenario_single_form_domain_associated, associate, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, permission)

    job = create_job(name='job', form=form, creator=user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        job = Job.query().get(job_id)
        riberry.model.conn.delete(job)
        riberry.model.conn.commit()
        assert not Job.query().get(job_id)


@pytest.mark.parametrize(['permission'], [
    [None],
    [FormDomain.PERM_JOB_DELETE_SELF],
])
def test_form_domain_user_with_no_access_to_delete_other_job(scenario_single_form_domain_associated, associate, create_user, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)
    if permission:
        associate(group, permission)

    job = create_job(name='job', form=form, creator=other_user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        job = Job.query().get(job_id)
        with pytest.raises(riberry.exc.AuthorizationError):
            riberry.model.conn.delete(job)
            riberry.model.conn.commit()


def test_form_domain_user_with_access_to_delete_other_job(scenario_single_form_domain_associated, associate, create_user, create_job):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_DELETE)

    job = create_job(name='job', form=form, creator=other_user)
    job_id = job.id

    with riberry.services.policy.policy_scope(user):
        job = Job.query().get(job_id)
        riberry.model.conn.delete(job)
        riberry.model.conn.commit()
        assert not Job.query().get(job_id)


def test_form_domain_user_with_no_access_to_execute_own_job(scenario_single_form_domain_associated, associate, create_job):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)

    job = create_job(name='job', form=form, creator=user)

    with riberry.services.policy.policy_scope(user):
        with pytest.raises(riberry.exc.AuthorizationError):
            execution = JobExecution(job=job, creator=user)
            riberry.model.conn.add(execution)
            riberry.model.conn.commit()


@pytest.mark.parametrize(['permission'], [
    [FormDomain.PERM_JOB_EXECUTE_SELF],
    [FormDomain.PERM_JOB_EXECUTE],
])
def test_form_domain_user_with_access_to_execute_own_job(scenario_single_form_domain_associated, associate, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, permission)

    job = create_job(name='job', form=form, creator=user)

    with riberry.services.policy.policy_scope(user):
        execution = JobExecution(job=job, creator=user)
        riberry.model.conn.add(execution)
        riberry.model.conn.commit()


@pytest.mark.parametrize(['permission'], [
    [None],
    [FormDomain.PERM_JOB_EXECUTE_SELF],
])
def test_form_domain_user_with_no_access_to_execute_other_job(scenario_single_form_domain_associated, associate, create_user, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)
    if permission:
        associate(group, permission)

    job = create_job(name='job', form=form, creator=other_user)

    with riberry.services.policy.policy_scope(user):
        with pytest.raises(riberry.exc.AuthorizationError):
            execution = JobExecution(job=job, creator=user)
            riberry.model.conn.add(execution)
            riberry.model.conn.commit()


def test_form_domain_user_with_access_to_execute_other_job(scenario_single_form_domain_associated, associate, create_user, create_job):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_EXECUTE)

    job = create_job(name='job', form=form, creator=other_user)

    with riberry.services.policy.policy_scope(user):
        execution = JobExecution(job=job, creator=user)
        riberry.model.conn.add(execution)
        riberry.model.conn.commit()


def test_form_domain_user_with_no_access_to_schedule_own_job(scenario_single_form_domain_associated, associate, create_job):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)

    job = create_job(name='job', form=form, creator=user)

    with riberry.services.policy.policy_scope(user):
        with pytest.raises(riberry.exc.AuthorizationError):
            schedule = JobSchedule(job=job, creator=user, cron='* * * * *')
            riberry.model.conn.add(schedule)
            riberry.model.conn.commit()


@pytest.mark.parametrize(['permission'], [
    [FormDomain.PERM_JOB_SCHEDULE_SELF],
    [FormDomain.PERM_JOB_SCHEDULE],
])
def test_form_domain_user_with_access_to_schedule_own_job(scenario_single_form_domain_associated, associate, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, permission)

    job = create_job(name='job', form=form, creator=user)

    with riberry.services.policy.policy_scope(user):
        schedule = JobSchedule(job=job, creator=user, cron='* * * * *')
        riberry.model.conn.add(schedule)
        riberry.model.conn.commit()


@pytest.mark.parametrize(['permission'], [
    [None],
    [FormDomain.PERM_JOB_SCHEDULE_SELF],
])
def test_form_domain_user_with_no_access_to_schedule_other_job(scenario_single_form_domain_associated, associate, create_user, create_job, permission):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)
    if permission:
        associate(group, permission)

    job = create_job(name='job', form=form, creator=other_user)

    with riberry.services.policy.policy_scope(user):
        with pytest.raises(riberry.exc.AuthorizationError):
            schedule = JobSchedule(job=job, creator=user, cron='* * * * *')
            riberry.model.conn.add(schedule)
            riberry.model.conn.commit()


def test_form_domain_user_with_access_to_schedule_other_job(scenario_single_form_domain_associated, associate, create_user, create_job):
    group, user, form = scenario_single_form_domain_associated
    other_user = create_user('other_user')
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_SCHEDULE)

    job = create_job(name='job', form=form, creator=other_user)

    with riberry.services.policy.policy_scope(user):
        schedule = JobSchedule(job=job, creator=user, cron='* * * * *')
        riberry.model.conn.add(schedule)
        riberry.model.conn.commit()


def test_form_domain_user_with_no_access_to_delete_own_job_execution_and_children(scenario_single_form_domain_associated, associate, create_job):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)

    job = create_job(name='job', form=form, creator=user)
    execution = JobExecution(job=job, creator=user)
    riberry.model.conn.add(execution)
    execution.streams.append(
        JobExecutionStream(
            name='stream',
            task_id='JobExecutionStream',
            steps=[
                JobExecutionStreamStep(
                    name='step',
                    task_id='JobExecutionStreamStep',
                )
            ]
        )
    )
    riberry.model.conn.commit()

    with riberry.services.policy.policy_scope(user):
        with pytest.raises(riberry.exc.AuthorizationError):
            riberry.model.conn.delete(execution)
            riberry.model.conn.commit()


def test_form_domain_user_with_access_to_delete_own_job_execution_and_children(scenario_single_form_domain_associated, associate, create_job):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_DELETE)

    job = create_job(name='job', form=form, creator=user)
    execution = JobExecution(job=job, creator=user)
    riberry.model.conn.add(execution)
    execution.streams.append(
        JobExecutionStream(
            name='stream',
            task_id='JobExecutionStream',
            steps=[
                JobExecutionStreamStep(
                    name='step',
                    task_id='JobExecutionStreamStep',
                )
            ]
        )
    )
    riberry.model.conn.commit()

    with riberry.services.policy.policy_scope(user):
        riberry.model.conn.delete(execution)
        riberry.model.conn.commit()


def test_form_domain_user_with_no_access_to_prioritize_execution_on_creation(scenario_single_form_domain_associated, associate, create_job):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_EXECUTE)

    job = create_job(name='job', form=form, creator=user)

    with riberry.services.policy.policy_scope(user):
        with pytest.raises(riberry.exc.AuthorizationError):
            execution = JobExecution(job=job, creator=user, priority=1)
            riberry.model.conn.add(execution)
            riberry.model.conn.commit()


def test_form_domain_user_with_access_to_prioritize_execution_on_creation(scenario_single_form_domain_associated, associate, create_job):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_EXECUTE)
    associate(group, FormDomain.PERM_JOB_PRIORITIZE)

    job = create_job(name='job', form=form, creator=user)

    with riberry.services.policy.policy_scope(user):
        execution = JobExecution(job=job, creator=user, priority=1)
        riberry.model.conn.add(execution)
        riberry.model.conn.commit()


def test_form_domain_user_with_no_access_to_prioritize_execution_on_update(scenario_single_form_domain_associated, associate, create_job, create_execution):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_UPDATE)
    associate(group, FormDomain.PERM_JOB_EXECUTE)

    job = create_job(name='job', form=form, creator=user)
    execution = create_execution(job)

    with riberry.services.policy.policy_scope(user):
        with pytest.raises(riberry.exc.AuthorizationError):
            execution.priority = 1
            riberry.model.conn.add(execution)
            riberry.model.conn.commit()


def test_form_domain_user_with_access_to_prioritize_execution_on_update(scenario_single_form_domain_associated, associate, create_job, create_execution):
    group, user, form = scenario_single_form_domain_associated
    associate(group, FormDomain.PERM_JOB_READ)
    associate(group, FormDomain.PERM_JOB_EXECUTE)
    associate(group, FormDomain.PERM_JOB_PRIORITIZE)

    job = create_job(name='job', form=form, creator=user)
    execution = create_execution(job)

    with riberry.services.policy.policy_scope(user):
        execution.priority = 1
        riberry.model.conn.add(execution)
        riberry.model.conn.commit()

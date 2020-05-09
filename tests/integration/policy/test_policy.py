import pytest

import riberry
from riberry.model.application import Application, ApplicationInstance, Heartbeat, ApplicationInstanceSchedule
from riberry.model.auth import User
from riberry.model.group import Group, GroupPermission
from riberry.model.interface import Form, InputDefinition, InputValueDefinition, InputValueEnum, InputFileDefinition
from riberry.policy.permissions import FormDomain, SystemDomain, ApplicationDomain


@pytest.fixture
def scenario_single_group_user_form(create_user, create_group, create_form):
    group: Group = create_group('group', permissions=[])
    user: User = create_user('user')
    form: Form = create_form('form')
    yield group, user, form


@pytest.fixture
def scenario_single_form_domain_unassociated(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    group.permissions.append(GroupPermission(name=FormDomain.PERM_ACCESS))
    associate(group, user)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_single_form_domain_associated(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    group.permissions.append(GroupPermission(name=FormDomain.PERM_ACCESS))
    associate(group, user)
    associate(group, form)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_single_sys_domain(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    group.permissions.append(GroupPermission(name=SystemDomain.PERM_ACCESS))
    associate(group, user)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_single_app_domain_unassociated(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    group.permissions.append(GroupPermission(name=ApplicationDomain.PERM_ACCESS))
    associate(group, user)
    yield scenario_single_group_user_form


@pytest.fixture
def scenario_single_app_domain_associated(scenario_single_group_user_form, associate):
    group, user, form = scenario_single_group_user_form
    group.permissions.append(GroupPermission(name=ApplicationDomain.PERM_ACCESS))
    associate(group, user)
    associate(group, form.application)
    yield scenario_single_group_user_form


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
    (InputFileDefinition, None, _query_all),
    (InputFileDefinition, lambda f: f.input_definition.id, _query_get),
    (InputFileDefinition, lambda f: f.input_definition.id, _query_filter_by),
    (InputFileDefinition, lambda f: f.input_definition.id, _query_filter),
    (InputValueDefinition, None, _query_all),
    (InputValueDefinition, lambda f: f.input_definition.id, _query_get),
    (InputValueDefinition, lambda f: f.input_definition.id, _query_filter_by),
    (InputValueDefinition, lambda f: f.input_definition.id, _query_filter),
    (InputValueEnum, None, _query_all),
    (InputValueEnum, lambda f: f.input_definition.id, _query_get),
    (InputValueEnum, lambda f: f.input_definition.id, _query_filter_by),
    (InputValueEnum, lambda f: f.input_definition.id, _query_filter),
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


def test_form_domain_user_with_access_to_builtin_instance_schedules(scenario_single_form_domain_associated):
    group, user, form = scenario_single_form_domain_associated
    group.permissions.append(GroupPermission(name=FormDomain.PERM_APP_SCHEDULES_READ_BUILTIN))
    with riberry.services.policy.policy_scope(user):
        assert [sched.parameter for sched in _query_all(ApplicationInstanceSchedule)] == ['active']


def test_form_domain_user_with_access_to_all_instance_schedules(scenario_single_form_domain_associated):
    group, user, form = scenario_single_form_domain_associated
    group.permissions.append(GroupPermission(name=FormDomain.PERM_APP_SCHEDULES_READ))
    with riberry.services.policy.policy_scope(user):
        assert [sched.parameter for sched in _query_all(ApplicationInstanceSchedule)] == ['active', 'custom']

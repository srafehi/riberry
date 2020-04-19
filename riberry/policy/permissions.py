from typing import Set


class MetaPermissions(type):

    permission_prefix = 'PERM_'

    def __new__(mcs, name, bases, dict_):
        permissions = set()
        actions = {
            action for base in bases
            if isinstance(base, MetaPermissions)
            for action in base.actions
        }

        for key, value in dict_.items():
            if key.startswith(mcs.permission_prefix) and dict_[key] is not None:
                actions.add(dict_[key])

        for action in actions:
            permission = f'{name}.{action}'
            dict_[f'{mcs.permission_prefix}{action}'] = permission
            actions.add(action)
            permissions.add(permission)

        instance = type.__new__(mcs, name, bases, dict_)
        instance._permissions = permissions
        instance._actions = actions
        return instance

    @property
    def permissions(self) -> Set:
        return self._permissions

    @property
    def actions(self) -> Set:
        return self._actions


class PermissionsBase(metaclass=MetaPermissions):
    __domain__ = None


class FormDomain(PermissionsBase):
    __domain__ = 'Form'

    PERM_ACCESS = 'ACCESS'
    PERM_JOB_READ_SELF = 'JOB_READ_SELF'
    PERM_JOB_CREATE_SELF = 'JOB_CREATE_SELF'
    PERM_JOB_UPDATE_SELF = 'JOB_UPDATE_SELF'
    PERM_JOB_DELETE_SELF = 'JOB_DELETE_SELF'
    PERM_JOB_EXECUTE_SELF = 'JOB_EXECUTE_SELF'
    PERM_JOB_SCHEDULE_SELF = 'JOB_SCHEDULE_SELF'
    PERM_JOB_READ = 'JOB_READ'
    PERM_JOB_CREATE = 'JOB_CREATE'
    PERM_JOB_UPDATE = 'JOB_UPDATE'
    PERM_JOB_DELETE = 'JOB_DELETE'
    PERM_JOB_EXECUTE = 'JOB_EXECUTE'
    PERM_JOB_SCHEDULE = 'JOB_SCHEDULE'
    PERM_JOB_PRIORITIZE = 'JOB_PRIORITIZE'
    PERM_APP_SCHEDULES_READ_BUILTIN = 'APP_SCHEDULES_READ_BUILTIN'


class ApplicationDomain(PermissionsBase):
    __domain__ = 'Application'

    PERM_ACCESS = 'ACCESS'
    PERM_APP_SCHEDULES_MANAGE = 'APP_SCHEDULES_MANAGE'


class SystemDomain(PermissionsBase):
    __domain__ = 'System'

    PERM_ACCESS = 'ACCESS'
    PERM_CAPACITY_CONFIG_MANAGE = 'CAPACITY_CONFIG_MANAGE'


PERMISSION_ROLES = {
    'FormDomain.ROLE_RESTRICTED': {
        FormDomain.PERM_ACCESS,
        FormDomain.PERM_APP_SCHEDULES_READ_BUILTIN,
        FormDomain.PERM_JOB_READ_SELF,
        FormDomain.PERM_JOB_CREATE_SELF,
        FormDomain.PERM_JOB_UPDATE_SELF,
        FormDomain.PERM_JOB_DELETE_SELF,
        FormDomain.PERM_JOB_EXECUTE_SELF,
        FormDomain.PERM_JOB_SCHEDULE_SELF,
    },
    'FormDomain.ROLE_BASIC': {
        FormDomain.PERM_ACCESS,
        FormDomain.PERM_APP_SCHEDULES_READ_BUILTIN,
        FormDomain.PERM_JOB_READ,
        FormDomain.PERM_JOB_CREATE_SELF,
        FormDomain.PERM_JOB_UPDATE_SELF,
        FormDomain.PERM_JOB_DELETE_SELF,
        FormDomain.PERM_JOB_EXECUTE_SELF,
        FormDomain.PERM_JOB_SCHEDULE_SELF,
    },
    'FormDomain.ROLE_ELEVATED': {
        FormDomain.PERM_ACCESS,
        FormDomain.PERM_APP_SCHEDULES_READ_BUILTIN,
        FormDomain.PERM_JOB_READ,
        FormDomain.PERM_JOB_CREATE,
        FormDomain.PERM_JOB_UPDATE,
        FormDomain.PERM_JOB_DELETE,
        FormDomain.PERM_JOB_EXECUTE,
        FormDomain.PERM_JOB_SCHEDULE,
    },
    'FormDomain.ROLE_MANAGER': {
        FormDomain.PERM_ACCESS,
        FormDomain.PERM_APP_SCHEDULES_READ_BUILTIN,
        FormDomain.PERM_JOB_READ,
        FormDomain.PERM_JOB_CREATE,
        FormDomain.PERM_JOB_UPDATE,
        FormDomain.PERM_JOB_DELETE,
        FormDomain.PERM_JOB_EXECUTE,
        FormDomain.PERM_JOB_SCHEDULE,
        FormDomain.PERM_JOB_PRIORITIZE,
    },
    'ApplicationDomain.ROLE_ADMINISTRATOR': {
        ApplicationDomain.PERM_ACCESS,
        ApplicationDomain.PERM_APP_SCHEDULES_MANAGE,
    },
    'SystemDomain.ROLE_ADMINISTRATOR': {
        SystemDomain.PERM_ACCESS,
        SystemDomain.PERM_CAPACITY_CONFIG_MANAGE,
    }
}

if __name__ == '__main__':
    for _key, _values in PERMISSION_ROLES.items():
        print('::', _key)
        for _value in sorted(_values):
            print('   >', _value)
        print()

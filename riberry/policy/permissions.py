from typing import Set


class MetaPermissions(type):

    def __new__(mcs, name, bases, dict_):
        permissions = set()
        actions = {
            action for base in bases
            if isinstance(base, MetaPermissions)
            for action in base.actions
        }

        for key, value in dict_.items():
            if key.startswith('PERM_') and dict_[key] is not None:
                actions.add(dict_[key])

        for action in actions:
            permission = f'{name}.{action}'
            dict_[f'PERM_{action}'] = permission
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

    @property
    def own_permissions(self) -> Set:
        return {permission for permission in self.permissions if permission.endswith('_SELF')}


class PermissionsBase(metaclass=MetaPermissions):
    pass


class CrudPermissions(PermissionsBase):
    PERM_CREATE = 'CREATE'
    PERM_READ = 'READ'
    PERM_UPDATE = 'UPDATE'
    PERM_DELETE = 'DELETE'


class OwnPermissions(PermissionsBase):
    PERM_READ_SELF = 'READ_SELF'
    PERM_UPDATE_SELF = 'UPDATE_SELF'
    PERM_DELETE_SELF = 'DELETE_SELF'


class Job(CrudPermissions, OwnPermissions):
    PERM_EXECUTE = 'EXECUTE'
    PERM_SCHEDULE = 'SCHEDULE'
    PERM_PRIORITIZE = 'PRIORITIZE'

    PERM_EXECUTE_SELF = 'EXECUTE_SELF'
    PERM_SCHEDULE_SELF = 'SCHEDULE_SELF'


class ApplicationInstanceSchedule(CrudPermissions):
    PERM_READ_BUILTIN = 'READ_BUILTIN'


PERMISSION_ROLES = {
    'FormRole.RESTRICTED': {
        ApplicationInstanceSchedule.PERM_READ_BUILTIN,
        Job.PERM_CREATE,
        *Job.own_permissions,
    },
    'FormRole.BASIC': {
        ApplicationInstanceSchedule.PERM_READ_BUILTIN,
        Job.PERM_READ,
        Job.PERM_CREATE,
        *Job.own_permissions - {Job.PERM_READ_SELF, Job.PERM_PRIORITIZE},
    },
    'FormRole.ELEVATED': {
        ApplicationInstanceSchedule.PERM_READ_BUILTIN,
        *Job.permissions - Job.own_permissions - {Job.PERM_PRIORITIZE},
    },
    'FormRole.MANAGER': {
        ApplicationInstanceSchedule.PERM_READ_BUILTIN,
        *Job.permissions - Job.own_permissions,
    },
    'ApplicationRole.MANAGER': {
        *ApplicationInstanceSchedule.permissions,
    }
}

if __name__ == '__main__':
    for _key, _values in PERMISSION_ROLES.items():
        print('::', _key)
        for _value in sorted(_values):
            print('   >', _value)

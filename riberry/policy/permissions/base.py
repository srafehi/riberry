from typing import Set


class _MetaPermissionDomain(type):
    permission_prefix = 'PERM_'

    def __new__(mcs, name, bases, dict_):
        permissions = set()
        actions = {
            action for base in bases
            if isinstance(base, _MetaPermissionDomain)
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

    def __contains__(self, item) -> bool:
        return item in self.permissions


class BasePermissionDomain(metaclass=_MetaPermissionDomain):
    PERM_ACCESS = 'ACCESS'

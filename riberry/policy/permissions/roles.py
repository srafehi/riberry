from .domain import FormDomain, ApplicationDomain, SystemDomain


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

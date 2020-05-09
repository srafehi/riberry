import riberry
from .domain import ApplicationDomain, FormDomain


CREATE = 'create'
READ = 'read'
UPDATE = 'update'
DELETE = 'delete'


_ACCESS_APP = {ApplicationDomain.PERM_ACCESS}
_ACCESS_FRM = {FormDomain.PERM_ACCESS}
_ACCESS_APP_FRM = _ACCESS_APP | _ACCESS_FRM

_CRUD_APP_FRM_READONLY = {READ: _ACCESS_APP_FRM}
_CRUD_FRM_READONLY = {READ: _ACCESS_FRM}
_CRUD_JOB = {
    READ: {FormDomain.PERM_JOB_READ},
    CREATE: {FormDomain.PERM_JOB_CREATE},
    UPDATE: {FormDomain.PERM_JOB_UPDATE},
    DELETE: {FormDomain.PERM_JOB_DELETE},
}
_CRUD_JOB_READONLY = {READ: {FormDomain.PERM_JOB_READ}}

CRUD_PERMISSIONS = {
    riberry.model.application.Application: _CRUD_APP_FRM_READONLY,
    riberry.model.application.ApplicationInstance: _CRUD_APP_FRM_READONLY,
    riberry.model.application.ApplicationInstanceSchedule: {
        READ: _ACCESS_APP | {
            FormDomain.PERM_APP_SCHEDULES_READ_BUILTIN,
            FormDomain.PERM_APP_SCHEDULES_READ,
        },
        CREATE: {ApplicationDomain.PERM_APP_SCHEDULES_MANAGE},
        UPDATE: {ApplicationDomain.PERM_APP_SCHEDULES_MANAGE},
        DELETE: {ApplicationDomain.PERM_APP_SCHEDULES_MANAGE},
    },
    riberry.model.application.Heartbeat: _CRUD_APP_FRM_READONLY,

    riberry.model.interface.Form: _CRUD_FRM_READONLY,
    riberry.model.interface.InputDefinition: _CRUD_FRM_READONLY,
    riberry.model.interface.InputValueDefinition: _CRUD_FRM_READONLY,
    riberry.model.interface.InputValueEnum: _CRUD_FRM_READONLY,
    riberry.model.interface.InputFileDefinition: _CRUD_FRM_READONLY,
    riberry.model.interface.InputValueInstance: _CRUD_JOB,
    riberry.model.interface.InputFileInstance: _CRUD_JOB,

    riberry.model.job.Job: _CRUD_JOB,
    riberry.model.job.JobExecution: {
        **_CRUD_JOB,
        **{CREATE: {FormDomain.PERM_JOB_EXECUTE}},
    },
    riberry.model.job.JobSchedule: {
        **_CRUD_JOB,
        **{CREATE: {FormDomain.PERM_JOB_SCHEDULE}},
    },
    riberry.model.job.JobExecutionReport: _CRUD_JOB_READONLY,
    riberry.model.job.JobExecutionExternalTask: _CRUD_JOB_READONLY,
    riberry.model.job.JobExecutionProgress: _CRUD_JOB_READONLY,
    riberry.model.job.JobExecutionMetric: _CRUD_JOB_READONLY,
    riberry.model.job.JobExecutionArtifact: _CRUD_JOB_READONLY,
    riberry.model.job.JobExecutionArtifactBinary: _CRUD_JOB_READONLY,
    riberry.model.job.JobExecutionArtifactData: _CRUD_JOB_READONLY,
    riberry.model.job.JobExecutionStream: _CRUD_JOB_READONLY,
    riberry.model.job.JobExecutionStreamStep: _CRUD_JOB_READONLY,
}

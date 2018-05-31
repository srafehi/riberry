import {types} from "mobx-state-tree"


const idType = types.identifier(types.union(types.number, types.string));

export const UserDetails = types.model({
    id: idType,
    name: types.string,
    email: types.string,
    department: types.optional(types.string, ''),
});

export const User = types.model({
    id: idType,
    userName: types.string,
    details: types.maybe(types.late(() => UserDetails)),
    groups: types.optional(types.array(types.late(() => Group)), [])
});

export const Application = types.model({
    id: idType,
    name: types.string,
    internalName: types.string,
    type: types.optional(types.string, ''),
    description: types.maybe(types.string),
    interfaces: types.optional(types.array(types.late(() => ApplicationInterface)), [])
});

export const ApplicationInterface = types.model({
    id: idType,
    name: types.string,
    internalName: types.string,
    version: types.number,
    description: types.maybe(types.string),
    inputFiles: types.optional(types.array(types.late(() => InputFileDefinition)), []),
    inputValues: types.optional(types.array(types.late(() => InputValueDefinition)), []),
});

export const InputFileDefinition = types.model({
    name: types.string,
    type: types.string,
    internalName: types.string,
    description: types.maybe(types.string),
    required: types.boolean,
});

export const InputValueDefinition = types.model({
    name: types.string,
    type: types.string,
    internalName: types.string,
    description: types.maybe(types.string),
    required: types.boolean,
    default: types.frozen,
    enumerations: types.optional(types.array(types.frozen), []),
});

export const ApplicationInstance = types.model({
    id: idType,
    name: types.string,
    internalName: types.string,
    heartbeat: types.maybe(types.late(() => Heartbeat)),
    interfaces: types.optional(types.array(types.late(() => ApplicationInstance)), []),
    application: types.maybe(types.late(() => Application))
});

export const Heartbeat = types.model({
    created: types.string,
    updated: types.string,
});

export const Form = types.model({
    id: idType,
    instance: types.maybe(types.late(() => ApplicationInstance)),
    interface: types.late(() => ApplicationInterface),
    jobs: types.maybe(types.array(types.late(() => Job))),
});

export const Job = types.model({
    id: idType,
    name: types.string,
    created: types.string,
    creator: types.maybe(types.late(() => User)),
    instance: types.maybe(types.late(() => ApplicationInstance)),
    interface: types.maybe(types.late(() => ApplicationInterface)),
    executions: types.maybe(types.array(types.late(() => JobExecution))),
});

export const JobExecution = types.model({
    id: idType,
    status: types.string,
    job: types.late(() => Job)
});

export const Group = types.model({
    id: idType,
    name: types.string,
});


export const JobSummary = types.model({
    RECEIVED: types.optional(types.number, 0),
    READY: types.optional(types.number, 0),
    QUEUED: types.optional(types.number, 0),
    ACTIVE: types.optional(types.number, 0),
    SUCCESS: types.optional(types.number, 0),
    FAILURE: types.optional(types.number, 0),
});
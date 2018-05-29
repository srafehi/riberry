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
    description: types.optional(types.string, ''),
    interfaces: types.optional(types.array(types.late(() => ApplicationInterface)), [])
});

export const ApplicationInterface = types.model({
    id: idType,
    name: types.string,
    internalName: types.string,
    version: types.number
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
    instance: types.late(() => ApplicationInstance),
    interface: types.late(() => ApplicationInterface)
});

export const Job = types.model({
    id: idType,
    name: types.string,
    instance: types.maybe(types.late(() => ApplicationInstance)),
    interface: types.maybe(types.late(() => ApplicationInterface)),
    job: types.maybe(types.late(() => Form)),
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
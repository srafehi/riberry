import {flow, types} from "mobx-state-tree"
import api from "../../api/api";
import {Form, JobExecution} from "../models";


let interval = null;

const loadForms = self => flow(function* () {
    const [formResponse, selfResponse] = yield Promise.all([
        api.forms.get({expand: ['interface', 'schedules', 'instance.schedules', 'instance.application', 'instance.heartbeat']}),
        api.self.profile({expand: ['executions.job.interface']})
    ]);
    self.forms = formResponse.data;
    self.executions = selfResponse.data.executions;
});

const setup = self => async () => {
    await self.loadForms();
    interval = setInterval(self.loadForms, 2000);
};


const tearDown = () => {
    clearInterval(interval);
};


const storeActions = self => ({
    loadForms: loadForms(self),
    setup: setup(self),
    tearDown: tearDown
});


const storeViews = self => ({});


const DashboardStore = types.model({
    forms: types.array(types.late(() => Form)),
    executions: types.array(types.late(() => JobExecution))
}).actions(storeActions).views(storeViews);

export const dashboardStore = DashboardStore.create({
    forms: [],
    executions: []
});
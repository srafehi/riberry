import {flow, types} from "mobx-state-tree"
import api from "../../api/api";
import {Form, JobExecution, JobSummary} from "../models";
import {injectInterval} from "../util";


const loadForms = self => flow(function* () {
    const [formResponse, selfResponse, jobResponse] = yield Promise.all([
        api.forms.getAll({expand: ['interface', 'schedules', 'instance.schedules', 'instance.application', 'instance.heartbeat']}),
        api.self.profile({expand: ['executions.job.interface']}),
        api.jobs.summary()
    ]);
    self.forms = formResponse.data;
    self.executions = selfResponse.data.executions;
    self.summary = jobResponse.data;
});


const storeActions = self => injectInterval(self, (s) => s.loadForms, {
    loadForms: loadForms(self)
});


const storeViews = self => ({});


const DashboardStore = types.model({
    forms: types.array(types.late(() => Form)),
    executions: types.array(types.late(() => JobExecution)),
    summary: types.late(() => JobSummary),
}).actions(storeActions).views(storeViews);

export const dashboardStore = DashboardStore.create({
    forms: [],
    executions: [],
    summary: JobSummary.create({}),
});
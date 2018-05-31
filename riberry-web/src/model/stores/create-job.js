import {getSnapshot, applySnapshot, types, flow} from "mobx-state-tree"
import {Form} from "../models";
import api from "../../api/api";
import {injectReset} from "../util";


const loadForm = self => flow(function* (formId) {
    const response = yield api.forms.get({id: formId, expand: ['interface.inputFiles', 'interface.inputValues.enumerations']});
    self.form = response.data;
});

const storeActions = self => injectReset(self, {
    loadForm: loadForm(self)
});

const storeViews = self => ({});

const CreateJobStore = types.model({
    form: types.maybe(types.late(() => Form))
}).actions(storeActions).views(storeViews);


export const createJobStore = CreateJobStore.create({
    form: null
});

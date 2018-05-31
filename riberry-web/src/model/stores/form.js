import {getSnapshot, applySnapshot, types, flow} from "mobx-state-tree"
import {Form} from "../models";
import api from "../../api/api";
import {injectReset, injectInterval} from "../util";


const loadForm = self => flow(function* ({formId}) {
    const response = yield api.forms.get({
        id: formId,
        expand: ['jobs.creator.details', 'interface.document']
    });
    self.form = response.data;
});

const storeActions = self => injectReset(self, injectInterval(self, (self) => self.loadForm, {
    loadForm: loadForm(self)
}));

const storeViews = self => ({});

const FormStore = types.model({
    form: types.maybe(types.late(() => Form))
}).actions(storeActions).views(storeViews);


export const formStore = FormStore.create({
    form: null
});

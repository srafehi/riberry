import {applySnapshot, getSnapshot} from "mobx-state-tree";


export const injectInterval = (self, func, actions) => {
    let interval = null;
    return {
        ...actions,
        setup: async (props) => {
            console.debug('creating interval', self, self.interval);
            const partialFunc = func(self);
            await partialFunc(props);
            interval = setInterval(() => partialFunc(props), self.interval || 5000);
        },
        tearDown: () => {
            clearInterval(interval);
            console.debug('clearing interval', self);
            interval = null;
        }
    };
};


export const injectReset = (self, actions) => {
    let initialState = {};
    return {
        ...actions,
        afterCreate: () => {
            actions.afterCreate && actions.afterCreate();
            initialState = getSnapshot(self);
        },
        reset: () => {
            actions.reset && actions.reset();
            applySnapshot(self, initialState);
        },
    }
};
import React from 'react';
import Loadable from 'react-loadable';


const Loading = () => (
    <div>Loading...</div>
);


const makeLoadable = (loader, loadingComponent=Loading) => ({
    loader,
    loadable: Loadable({
        loader,
        loading: loadingComponent
    })
});

const pages = {
    dashboard: makeLoadable(() => import('../pages/dashboard/Dashboard')),
    forms: makeLoadable(() => import('../pages/forms/Forms'))
};

export default pages;


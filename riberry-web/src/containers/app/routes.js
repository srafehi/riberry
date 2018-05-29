import loadables from './loadables';


const loadableMixin = ({loader, loadable}) => ({loader, component: loadable});

const routes = [
    {
        ... loadableMixin(loadables.dashboard),
        path: '/',
        exact: true
    },
    {
        ... loadableMixin(loadables.dashboard),
        path: '/dashboard',
        exact: true
    },
    {
        ... loadableMixin(loadables.forms),
        path: '/forms/:formId',
        exact: false
    }
];

export default routes;

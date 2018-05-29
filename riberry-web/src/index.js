import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import registerServiceWorker from './registerServiceWorker';
import createHistory from 'history/createBrowserHistory'
import {Router} from 'react-router-dom'
import {userStore} from "./model/stores/user";
import {dashboardStore} from "./model/stores/dashboard";
import {Provider} from "mobx-react";

const history = createHistory();


ReactDOM.render((
    <Router history={history}>
        <Provider userStore={userStore} dashboardStore={dashboardStore}>
            <App/>
        </Provider>
    </Router>
), document.getElementById('root'));
registerServiceWorker();


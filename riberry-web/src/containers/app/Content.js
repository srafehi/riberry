import React from 'react';
import {Route, Switch} from 'react-router-dom';
import routes from './routes';


export const Content = () => {
    return (
        <Switch>
            {routes.map(r => (
                <Route
                    key={`${r.path}:${r.component.toString()}`}
                    exact={!!r.exact}
                    path={r.path}
                    component={r.component}/>
            ))}
        </Switch>
    )
};
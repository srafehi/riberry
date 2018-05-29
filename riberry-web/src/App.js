import React from 'react';

import {RootContainer} from "./components/app/RootContainer";
import {NavigationBar} from "./components/app/NavigationBar";
import {NagivationDrawer} from "./components/app/NagivationDrawer";
import {Content} from "./containers/app/Content";
import {Login} from "./containers/pages/login/Login";
import {inject, observer} from 'mobx-react';
import {withRouter} from "react-router-dom";

const MainApp = () => (
    <RootContainer>
        <NavigationBar/>
        <NagivationDrawer menuItems={[
            {title: 'Dashboard', to: '/dashboard'},
            {title: 'Forms', to: '/forms'},
            {title: 'Jobs', to: '/jobs'}
        ]}/>
        <Content/>
    </RootContainer>
);


@inject('userStore')
@withRouter
@observer
class App extends React.Component {
    render() {
        const {userStore} = this.props;
        if (userStore.initialLoad) {
            return <div>loading...</div>
        } else {
            return userStore.loggedIn ? <MainApp/> : <Login/>
        }

    }
}

export default App;

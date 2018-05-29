import React from 'react';
import {withStyles} from '@material-ui/core/styles';
import {SimpleIconButton} from "../helpers";
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import {Menu, MenuItem} from "@material-ui/core";
import {inject, observer} from 'mobx-react';

const styles = {
    root: {
        flexGrow: 1,
        boxShadow: 'none !important',
        zIndex: 10
    },
    title: {
        flex: 1
    }
};


@inject('userStore')
class UserMenu extends React.Component {
    state = {
        anchorEl: null
    };

    onMenuButtonClick = event => {
        this.setState({anchorEl: event.currentTarget});
    };

    onMenuClosed = () => {
        this.setState({anchorEl: null});
    };

    onLogout = () => {
        this.props.userStore.logout();
        this.onMenuClosed();
    };

    render() {
        return [
            <SimpleIconButton key={1} onClick={this.onMenuButtonClick} color="inherit" icon="account_circle"/>,
            <Menu
                key={2}
                open={Boolean(this.state.anchorEl)}
                anchorEl={this.state.anchorEl}
                onClose={this.onMenuClosed}
                anchorOrigin={{vertical: 'bottom', horizontal: 'center'}}
                transformOrigin={{vertical: 'top', horizontal: 'right'}}>
                <MenuItem onClick={this.onLogout}>Logout</MenuItem>
            </Menu>
        ]
    }
}


@inject('userStore')
@observer
@withStyles(styles)
export class NavigationBar extends React.Component {
    render() {
        const {classes, userStore} = this.props;
        return (
            <AppBar className={classes.root} position="absolute">
                <Toolbar>
                    <SimpleIconButton color="inherit" icon="menu"/>
                    <Typography className={classes.title} variant="title" color="inherit">Riberry</Typography>
                    {userStore.user.details.name}
                    <SimpleIconButton color="inherit" icon="notifications"/>
                    <UserMenu/>
                </Toolbar>
            </AppBar>
        )

    }
}
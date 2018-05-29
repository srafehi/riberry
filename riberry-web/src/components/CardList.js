import React from 'react';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import IconButton from '@material-ui/core/IconButton';
import Icon from '@material-ui/core/Icon';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemSecondaryAction from '@material-ui/core/ListItemSecondaryAction';
import Tooltip from '@material-ui/core/Tooltip';
import {Link} from "react-router-dom";


export class CardListItem extends React.Component {
    state = {
        anchorEl: null
    };

    onClick = () => {
        this.props.onClick && this.props.onClick();
    };

    onMenuItemClick = label => {
        this.onMenuClosed();
        this.props.onMenuItemClick && this.props.onMenuItemClick(label);
    };

    onMenuButtonClick = event => {
        this.setState({anchorEl: event.currentTarget});
    };

    onMenuClosed = () => {
        this.setState({anchorEl: null});
    };


    render() {
        const {primary, secondary = null, icon, iconColor, iconTip='', menuItems = [], to, onClick} = this.props;
        const {anchorEl} = this.state;
        console.log(this.state);

        let menu = null;
        if (menuItems && menuItems.length) {
            menu = (
                <ListItemSecondaryAction>
                    <IconButton onClick={this.onMenuButtonClick}>
                        <Icon>more_horiz</Icon>
                    </IconButton>
                    <Menu open={Boolean(anchorEl)} anchorEl={anchorEl} onClose={() => this.onMenuClosed()}>
                        {menuItems.map(item => (
                            <MenuItem key={item} onClick={() => this.onMenuItemClick(item)}>{item}</MenuItem>
                        ))}
                    </Menu>
                </ListItemSecondaryAction>
            );
        };

        const mixins = to ? {to: to, component: Link} : {onClick: this.onClick}


        return (
            <ListItem button {...mixins}>
                <Tooltip id="tooltip-left" title={iconTip} placement="left">
                    <ListItemIcon><Icon style={{color: iconColor}}>{icon}</Icon></ListItemIcon>
                </Tooltip>
                <ListItemText primary={primary} secondary={secondary}/>
                {menu}
            </ListItem>
        );

    }
}


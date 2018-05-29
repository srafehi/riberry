import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Icon from '@material-ui/core/Icon';
import ListItemIcon from '@material-ui/core/ListItemIcon';


export const SimpleIconButton = ({icon, ...props}) => (
    <IconButton {...props}>
        <Icon>{icon}</Icon>
    </IconButton>
);

export const SimpleListItemIcon = ({icon, ...props}) => (
    <ListItemIcon {...props}>
        <Icon>{icon}</Icon>
    </ListItemIcon>
);
import React from 'react';
import {withStyles} from '@material-ui/core/styles';
import Drawer from "@material-ui/core/Drawer";
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import {Link} from 'react-router-dom';


const style = {
    root: {
        position: 'relative',
        height: '100vh',
        zIndex: 0
    },
    paper: {
        width: '240px',
        paddingTop: '60px'
    }
};

@withStyles(style)
export class NagivationDrawer extends React.Component {

    render() {
        const {classes, menuItems = []} = this.props;
        return (
            <Drawer className={classes.root} classes={{paper: classes.paper}} variant="permanent">
                <List>
                    {menuItems.map(({title, to}) => (
                        <ListItem key={to} button component={Link} to={to}>
                            <ListItemText primary={title}/>
                        </ListItem>
                    ))}
                </List>
            </Drawer>
        );
    }
}
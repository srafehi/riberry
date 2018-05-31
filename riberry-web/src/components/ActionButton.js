import React from 'react';
import {withStyles} from '@material-ui/core/styles';
import {Menu, MenuItem, Button, Icon} from '@material-ui/core';


const styles = theme => ({
    fab: {
        position: 'fixed',
        bottom: theme.spacing.unit * 8,
        right: theme.spacing.unit * 8
    }
});


@withStyles(styles)
export class ActionButton extends React.Component {

    render() {
        const {classes, onClick, children} = this.props;
        return (
            <Button variant="fab" color="secondary" className={classes.fab} onClick={onClick}>
                <Icon>add</Icon>
                {children}
            </Button>
        );
    }
}

export class ActionButtonMenu extends React.Component {
    state = {
        anchor: null
    };

    open = (evt) => this.setState({anchor: evt.currentTarget});

    close = () => this.setState({anchor: null});

    select = (option) => {
        this.close();
        this.props.onSelected(option);
    };

    render() {
        return (
            [
                <ActionButton onClick={(evt) => this.open(evt)}/>,
                <Menu
                    id="simple-menu"
                    anchorEl={this.state.anchor}
                    open={Boolean(this.state.anchor)}
                    onClose={() => this.close()}>
                    {this.props.options.map(o => (
                        <MenuItem key={o} onClick={() => this.select(o)}>{o}</MenuItem>
                    ))}
                </Menu>
            ]
        )
    }
}
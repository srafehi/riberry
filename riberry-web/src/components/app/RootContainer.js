import React from 'react';
import {withStyles} from '@material-ui/core/styles';


const style = {
    root: {
        flexGrow: 1,
        display: 'flex',
        position: 'relative',
    }
};

@withStyles(style)
export class RootContainer extends React.Component {
    render() {
        const {classes, children} = this.props;
        return (<div className={classes.root}>{children}</div>);
    }
}
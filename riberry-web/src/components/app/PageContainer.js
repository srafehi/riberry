import React from 'react';
import {withStyles} from '@material-ui/core/styles';
import Grid from "@material-ui/core/Grid";


const styles = {
    root: {
        flex: 1,
        marginLeft: 240,
        paddingTop: 96
    }
};


@withStyles(styles)
export class PageContainer extends React.Component {
    render() {
        const {classes, children} = this.props;
        return (
            <main className={classes.root}>
                <Grid container>
                    <Grid item xs={2}/>
                    <Grid item xs={8}>
                        {children}
                    </Grid>
                </Grid>
            </main>
        );
    }
}
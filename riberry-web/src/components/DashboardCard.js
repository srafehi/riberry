import React from 'react';
import {withStyles} from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import {Grid, Paper, Tab, Tabs} from '@material-ui/core';


const styles = theme => ({
    root: {
        backgroundColor: theme.palette.primary.main
    },
    panel: {
        backgroundColor: 'transparent'
    },
    text: {
        color: theme.palette.primary.contrastText
    }
});

const composeStyles = (styles, o) => withStyles(styles)(o);


export const DashboardCard = composeStyles(styles, ({classes, title, children}) => (
    <Paper className={classes.root} style={{padding: 16}}>
        <Grid container justify={'center'} spacing={16}>
            <Grid item xs={12}>
                <Typography variant="title" className={classes.text} gutterBottom>{title}</Typography>
            </Grid>
            {children}
        </Grid>
    </Paper>
));


export const DashboardCardPanel = composeStyles(styles, ({classes, title, value}) => (
    <Grid item xs>
        <Paper elevation={0} className={classes.panel}>
            <Typography className={classes.text} variant="caption">{title}</Typography>
            <Typography className={classes.text} variant="display1">{value}</Typography>
        </Paper>
    </Grid>
));
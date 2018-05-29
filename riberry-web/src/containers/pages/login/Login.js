import React from 'react';
import {RootContainer} from "../../../components/app/RootContainer";
import {Button, Grid, Paper, TextField, Typography} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';
import {inject} from 'mobx-react';

const styles = {
    textField: {
        marginBottom: 16,
    },
    paper: {
        padding: 24,
    },
};


@inject('userStore')
@withStyles(styles)
export class Login extends React.Component {
    state = {
        request: false,
        username: '',
        password: ''
    };

    handleUpdate = label => event => {
        this.setState({[label]: event.target.value});
    };

    onClick = async () => {
        this.setState({request: true});
        await this.props.userStore.login(this.state.username, this.state.password);
        this.setState({request: false});
    };

    render() {
        console.log(this.state);
        const {classes} = this.props;
        const {username, password, request} = this.state;

        return (
            <RootContainer>
                <Grid container justify={'center'} alignItems={'center'} direction={'row'}
                      style={{height: '100vh'}}>
                    <Grid item xs={3}>
                        <Paper className={classes.paper}>
                            <Typography variant="headline" align="center" gutterBottom>
                                Login to Riberry
                            </Typography>
                            <TextField
                                autoFocus
                                disabled={request}
                                className={classes.textField}
                                label='Username'
                                type='text'
                                value={username || ''}
                                onChange={this.handleUpdate('username')}
                                fullWidth/>
                            <TextField
                                disabled={request}
                                className={classes.textField}
                                label='Password'
                                type={'password'}
                                value={password || ''}
                                onChange={this.handleUpdate('password')}
                                fullWidth/>
                            <Button
                                disabled={request}
                                variant="raised"
                                size="large"
                                color='primary'
                                onClick={this.onClick}
                                fullWidth>Login</Button>
                        </Paper>
                    </Grid>
                </Grid>
            </RootContainer>
        );
    }
}

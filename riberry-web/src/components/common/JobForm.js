import React from 'react';
import {
    Button,
    Dialog,
    DialogActions,
    DialogContent,
    DialogContentText,
    Grid,
    Icon,
    MenuItem,
    TextField,
    Typography,
    FormControlLabel,
    Checkbox
} from "@material-ui/core";
import {inject, observer} from "mobx-react";
import {formatBytes} from "../../util";
import api from "../../api/api";

const InputValue = ({onChange, value, name, internalName, type, description, required, enumerations, ...other}) => {
    const props = {
        type,
        required,
        id: internalName,
        select: Boolean(enumerations && enumerations.length),
        onChange: (e) => onChange(e.target.value)
    };
    props.value = value === '' ? value : (value || other.default || undefined);

    if (['text', 'number'].includes(type)) {
        props.label = name;
        props.helperText = description;
    } else {
        props.helperText = `${required ? '* ' : ''}${name}`;
        props.helperText += description ? ` - ${description}` : '';
    }

    return (
        <Grid item xs={12}>
            <TextField {...props} fullWidth>
                {props.select ? enumerations.map(e => <MenuItem key={e} value={e}>{e}</MenuItem>) : null}
            </TextField>
        </Grid>
    );

};


const InputFile = ({onChange, file, name, internalName, type, description, required}) => {
    const props = {
        id: internalName,
        required: required,
        onChange: (e) => onChange(e.currentTarget.files.length ? e.currentTarget.files[0] : null)
    };

    return (
        <Grid item xs={12}>
            <Typography variant="subheading">{name}</Typography>
            <Typography variant="caption" gutterBottom>{description}</Typography>
            <input {...props} type="file" style={{display: 'none'}}/>
            <label htmlFor={internalName}>
                <Button variant="outlined" color="primary" component="span" fullWidth>
                    <Icon style={{marginRight: 12}}>cloud_upload</Icon> Upload
                </Button>
            </label>
            <Typography variant="caption"
                        align="right">{file ? `Selected ${file.name} (${formatBytes(file.size)})` : null}</Typography>
        </Grid>
    );

};


@inject('createJobStore')
@observer
export class JobForm extends React.Component {
    state = {
        executeNow: true
    };

    handleChange = (type, key) => value => {
        this.setState({[`${type}:${key}`]: value});
    };

    getValue = (type, key) => this.state[`${type}:${key}`];

    async componentDidMount() {
        this.props.createJobStore.reset();
        await this.props.createJobStore.loadForm(this.props.formId);
    }

    componentWillUnmount() {
        this.props.createJobStore.reset();
    }

    componentDidUpdate() {
        console.log(this.state);
    }

    onClose = () => {
        this.props.onClose && this.props.onClose();
    };

    onCreate = async () => {
        const formData = new FormData();

        const inputs = {};
        const appInterface = this.props.createJobStore.form.interface;
        appInterface.inputValues.forEach(i => {
            const value = this.state[`input:${i.name}`];
            inputs[i.name] = value === undefined ? i.default : value;
        });

        appInterface.inputFiles.forEach(i => {
            const file = this.state[`file:${i.name}`];
            if (file) {
                formData.append(i.name, file, file.name);
            }
        });

        formData.append('inputs', JSON.stringify(inputs));
        formData.append('jobName', this.state.jobName || '');
        this.state.executeNow && formData.append('executeNow', '1');
        await api.jobs.create({formId: this.props.formId, data: formData});
        this.onClose();
    };

    render() {

        const {createJobStore} = this.props;

        if (!createJobStore.form) {
            return <div/>
        }

        const appInterface = createJobStore.form.interface;

        return (
            <Dialog open={true} fullWidth maxWidth={'sm'}>
                <DialogContent>
                    <Typography variant="title" gutterBottom>{appInterface.name} [v{appInterface.version}]</Typography>
                    <Typography variant="subheading" gutterBottom>Create Job</Typography>
                    <DialogContentText style={{marginTop: 16}}gutterBottom>{appInterface.description}</DialogContentText>
                </DialogContent>
                <DialogContent>
                    <Grid container spacing={16}>
                        <Grid item xs={12}>
                            <TextField autoFocus id="jobName" label="Job Name" required type="text"
                                       value={this.state.jobName}
                                       onChange={e => this.setState({jobName: e.target.value})} fullWidth/>
                        </Grid>
                        {appInterface.inputValues.map(input => (
                            <InputValue key={input.name} {...input} value={this.getValue('input', input.name)}
                                        onChange={this.handleChange('input', input.name)}/>
                        ))}
                        {appInterface.inputFiles.map(input => (
                            <InputFile key={input.name} {...input} file={this.getValue('file', input.name)}
                                       onChange={this.handleChange('file', input.name)}/>
                        ))}
                        <Grid item xs={12}>
                            <FormControlLabel control={
                                <Checkbox
                                    checked={this.state.executeNow}
                                    onChange={e => this.setState({executeNow: e.target.checked})} />
                            } label="Execute Now" />
                        </Grid>
                    </Grid>
                </DialogContent>
                <DialogActions>
                    <Button color="primary" onClick={this.onClose}>Cancel</Button>
                    <Button color="primary" onClick={this.onCreate}>Create</Button>
                </DialogActions>
            </Dialog>
        );
    }

}
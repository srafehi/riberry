import React from 'react';
import {PageContainer} from "../../../components/app/PageContainer";
import {
    AppBar,
    Grid,
    Icon,
    Paper,
    Tab,
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableRow,
    Tabs,
    Typography,
    Dialog,
    DialogContent,
    DialogContentText,
    DialogTitle,
    DialogActions,
    Button,
    Divider,
    MenuItem,
    TextField
} from "@material-ui/core";
import {DashboardCard, DashboardCardPanel} from "../../../components/DashboardCard";
import {Link} from "react-router-dom";
import {JobForm} from "../../../components/common/JobForm";
import {inject, observer} from 'mobx-react';
import {ActionButtonMenu} from "../../../components/ActionButton";

const JobTable = ({form}) => (
    <Table>
        <TableHead>
            <TableRow>
                <TableCell padding="dense">Name</TableCell>
                <TableCell padding="dense">Owner</TableCell>
                <TableCell padding="dense">Created</TableCell>
            </TableRow>
        </TableHead>
        <TableBody>
            {
                form && form.jobs.map(job => (
                    <TableRow>
                        <TableCell padding="dense"><Link to={`/jobs/${job.id}`}>{job.name}</Link></TableCell>
                        <TableCell padding="dense">{job.creator.details.name}</TableCell>
                        <TableCell padding="dense">{job.created}</TableCell>
                    </TableRow>
                ))
            }
        </TableBody>
    </Table>
);


@inject('formStore')
@observer
export default class Forms extends React.Component {
    state = {
        viewJob: false
    };

    async componentWillMount() {
        console.log(this.props);
        const {formStore, match} = this.props;
        formStore.reset();
        formStore.setup({formId: match.params.formId});
    }

    componentWillUnmount() {
        const {formStore} = this.props;
        formStore.reset();
        formStore.tearDown();
    }

    onSelected = (option) => {
        if (option === 'Create Job') {
            this.setState({viewJob: true});
        }
    };

    render() {
        return (
            <PageContainer>
                <ActionButtonMenu options={['Create Job']} onSelected={this.onSelected} />
                {this.state.viewJob ? <JobForm formId={this.props.match.params.formId} onClose={
                    async () => {
                        await this.props.formStore.setup({formId: this.props.match.params.formId});
                        this.setState({viewJob: false});
                    }
                }/> : null}
                <Grid item xs={12}>
                    <DashboardCard title='Last 7 Days'>
                        <DashboardCardPanel title='Queued jobs' value={0}/>
                        <DashboardCardPanel title='Active jobs' value={0}/>
                        <DashboardCardPanel title='Successful jobs' value={0}/>
                        <DashboardCardPanel title='Failed jobs' value={0}/>
                    </DashboardCard>
                </Grid>
                <Grid item xs={12}>
                    <Grid item xs={12}>
                        <AppBar position="static">
                            <Tabs value={0}>
                                <Tab label="Jobs"/>
                                <Tab label="Job Schedules"/>
                                <Tab label="Form Schedules"/>
                                <Tab label="Documentation"/>
                            </Tabs>
                        </AppBar>
                        <Paper style={{backgroundColor: 'white'}}>
                            <JobTable form={this.props.formStore.form}/>

                            {/*<Table>*/}
                            {/*<TableHead>*/}
                            {/*<TableRow>*/}
                            {/*<TableCell padding="dense">Job</TableCell>*/}
                            {/*<TableCell padding="dense">Schedule</TableCell>*/}
                            {/*<TableCell padding="dense">Next Run</TableCell>*/}
                            {/*<TableCell padding="dense">Limit</TableCell>*/}
                            {/*</TableRow>*/}
                            {/*</TableHead>*/}
                            {/*<TableBody>*/}
                            {/*<TableRow>*/}
                            {/*<TableCell padding="dense"><Link to={'/jobs/1'}>2018-May-27: 3CBR-01-25</Link></TableCell>*/}
                            {/*<TableCell padding="dense">At 02:00 on Saturday</TableCell>*/}
                            {/*<TableCell padding="dense">12:34PM Monday, 24th May 2018 AEDT</TableCell>*/}
                            {/*<TableCell padding="dense">Unlimited</TableCell>*/}
                            {/*</TableRow>*/}
                            {/*</TableBody>*/}
                            {/*</Table>*/}
                        </Paper>
                    </Grid>
                </Grid>
                {/*<JobForm formId={this.props.match.params.formId}/>*/}
                <div>Forms page {this.props.match.params.formId}</div>
            </PageContainer>
        );
    }
}
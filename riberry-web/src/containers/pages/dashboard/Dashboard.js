import React from 'react';
import {PageContainer} from "../../../components/app/PageContainer";
import {DashboardCard, DashboardCardPanel} from "../../../components/DashboardCard";
import {Card, CardHeader, colors, Grid, List} from "@material-ui/core";

import {CardListItem} from "../../../components/CardList";
import {inject, observer} from 'mobx-react';
import {DateTime} from 'luxon';

@inject('dashboardStore')
@observer
export default class Dashboard extends React.Component {

    async componentWillMount() {
        const {dashboardStore} = this.props;
        dashboardStore.setup();
    }

    componentWillUnmount() {
        const {dashboardStore} = this.props;
        dashboardStore.tearDown();
    }

    static formIconSettings(form) {
        if (form.instance.heartbeat == null) {
            return {
                icon: 'error_outline',
                iconTip: 'Offline (never online)',
                iconColor: colors.blueGrey[600]
            }
        }

        const updatedDate = DateTime.fromISO(form.instance.heartbeat.updated);
        const currentDate = DateTime.utc();

        if ((currentDate - updatedDate) > 10000) {
            return {
                icon: 'error_outline',
                iconTip: 'Offline',
                iconColor: colors.red[600]
            }
        }

        return {
            icon: 'check_circle_outline',
            iconTip: 'Online',
            iconColor: colors.green[600]
        }
    }

    static execIconSettings(execution) {
        switch (execution.status) {
            case 'RECEIVED':
                return {icon: 'radio_button_unchecked'};
            case 'READY':
            case 'ACTIVE':
                return {icon: 'timelapse'};
            case 'SUCCESS':
                return {icon: 'check_circle_outline'};
            case 'FAILURE':
                return {icon: 'error_outline'};
            default:
                return {icon: 'help_outline'};
        }
    }

    render() {
        const {dashboardStore} = this.props;

        let jobExecutions = null;
        if (dashboardStore.executions.length) {
            jobExecutions = (
                <Grid item xs={5}>
                    <Card>
                        <CardHeader title="Job Executions"/>
                        <List dense>
                            {dashboardStore.executions.map(execution => (
                                <CardListItem
                                    {... Dashboard.execIconSettings(execution)}
                                    key={execution.id}
                                    iconTip={execution.status}
                                    iconColor={colors.blueGrey[600]}
                                    primary={execution.job.name}
                                    to={`/executions/${execution.id}`}
                                    secondary={execution.job.interface.name}/>
                            ))}
                        </List>
                    </Card>
                </Grid>
            );
        }

        return (
            <PageContainer>
                <Grid item xs={12}>
                    <DashboardCard title='Last 7 Days'>
                        <DashboardCardPanel title='Queued jobs'
                                            value={dashboardStore.summary.RECEIVED + dashboardStore.summary.READY + dashboardStore.summary.QUEUED}/>
                        <DashboardCardPanel title='Active jobs' value={dashboardStore.summary.ACTIVE}/>
                        <DashboardCardPanel title='Successful jobs' value={dashboardStore.summary.SUCCESS}/>
                        <DashboardCardPanel title='Failed jobs' value={dashboardStore.summary.FAILURE}/>
                    </DashboardCard>
                </Grid>
                <Grid item xs={dashboardStore.executions.length ? 7 : 12}>
                    <Card>
                        <CardHeader title="Forms"/>
                        <List dense>
                            {dashboardStore.forms.map(form => (
                                <CardListItem
                                    {... Dashboard.formIconSettings(form)}
                                    key={form.id}
                                    to={`/forms/${form.id}`}
                                    primary={form.interface.name} secondary={form.instance.name}
                                    menuItems={['Create Job']}/>
                            ))}
                        </List>
                    </Card>
                </Grid>
                {jobExecutions}
            </PageContainer>
        )
    }
}
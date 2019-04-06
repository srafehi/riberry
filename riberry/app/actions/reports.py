import riberry
from typing import List
from .. import env


def update_all_reports(app_instance=None):
    app_instance = app_instance if app_instance else env.get_instance_model()
    reports: List[riberry.model.job.JobExecutionReport] = riberry.model.job.JobExecutionReport.query().filter(
        riberry.model.job.JobExecutionReport.marked_for_refresh == True,
    ).join(
        riberry.model.job.JobExecution
    ).join(
        riberry.model.job.Job
    ).filter_by(
        instance=app_instance
    ).all()

    cxt = env.current_context
    for report in reports:
        root_id = report.job_execution.task_id

        with cxt.scope(root_id=root_id, task_id=root_id, stream=None, step=None, category=None):
            cxt.event_registry.call(event_type=cxt.event_registry.types.on_report_refresh, report=report.name)
            if report.marked_for_refresh:
                report.marked_for_refresh = False
                riberry.model.conn.commit()

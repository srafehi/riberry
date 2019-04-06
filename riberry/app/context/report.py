import json
from functools import partial

import riberry


class Report:

    def __init__(self, context):
        self.context: riberry.app.context.Context = context

    def model(self, name):
        job_execution = self.context.current.job_execution
        report: riberry.model.job.JobExecutionReport = riberry.model.job.JobExecutionReport.query().filter_by(
            job_execution=job_execution, name=name,
        ).first()

        if not report:
            self.context.data.execute_once(
                key=riberry.app.util.misc.internal_data_key(f'once.create_report.{name}'),
                func=partial(self._create_report, name=name, job_execution=job_execution)
            )
            return self.model(name=name)

        return report

    @staticmethod
    def _create_report(name, job_execution):
        report = riberry.model.job.JobExecutionReport(job_execution=job_execution, name=name)
        riberry.model.conn.add(report)
        riberry.model.conn.commit()

    def mark_for_refresh(self, name):
        report = self.model(name=name)
        report.marked_for_refresh = True
        riberry.model.conn.commit()

    def update(self, report, body, renderer=None):
        model = self.model(name=report)
        model.marked_for_refresh = False
        model.renderer = renderer or model.renderer
        model.report = json.dumps(body).encode()
        riberry.model.conn.commit()

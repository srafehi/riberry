import riberry
from typing import List
from .. import env


def update_all_data_items(app_instance=None):
    app_instance = app_instance if app_instance else env.get_instance_model()
    data_items: List[riberry.model.misc.ResourceData] = riberry.model.misc.ResourceData.query().filter(
        riberry.model.misc.ResourceData.marked_for_refresh == True,
        riberry.model.misc.ResourceData.resource_type == riberry.model.misc.ResourceType.job_execution,
    ).join(
        riberry.model.job.JobExecution,
        riberry.model.misc.ResourceData.resource_id == riberry.model.job.JobExecution.id,
    ).join(
        riberry.model.job.Job
    ).filter_by(
        instance=app_instance
    ).all()

    cxt = env.current_context
    for item in data_items:
        root_id = riberry.model.job.JobExecution.query().filter_by(id=item.resource_id).one().task_id

        with cxt.scope(root_id=root_id, task_id=root_id, stream=None, step=None, category=None):
            cxt.event_registry.call(event_type=cxt.event_registry.types.on_data_updated, data_name=item.name)
            cxt.event_registry.call(event_type=cxt.event_registry.types.on_report_refresh, data_name=item.name)
            if item.marked_for_refresh:
                item.marked_for_refresh = False
                riberry.model.conn.commit()

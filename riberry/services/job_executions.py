from riberry import model, policy


@policy.context.post_authorize(action='view')
def job_execution_by_id(execution_id):
    return model.job.JobExecution.query().filter_by(id=execution_id).one()


@policy.context.post_authorize(action='view')
def job_artifact_by_id(artifact_id):
    return model.job.JobExecutionArtifact.query().filter_by(id=artifact_id).one()

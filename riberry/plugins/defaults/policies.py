from riberry import policy, model
from riberry.policy import AttributeContext


class RootPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        return context.subject is not None


class UserPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        return True


class ApplicationPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return isinstance(context.resource, model.application.Application)

    def condition(self, context: AttributeContext) -> bool:
        return True


class ApplicationViewPolicy(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'view'

    def condition(self, context: AttributeContext) -> bool:
        return True


class ViewApplicationUsingFormRelationshipRule(policy.Rule):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        application: model.application.Application = context.resource

        for instance in application.instances:
            for form in instance.forms:
                if set(user.groups) & set(form.groups):
                    return True
        return False


class GenericCreatePolicy(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'create'

    def condition(self, context: AttributeContext) -> bool:
        return True


class RejectCreateRule(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        return False


class ApplicationInstancePolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return isinstance(context.resource, model.application.ApplicationInstance)

    def condition(self, context: AttributeContext) -> bool:
        return True


class ApplicationInstanceViewPolicy(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'view'

    def condition(self, context: AttributeContext) -> bool:
        return True


class ViewApplicationInstanceUsingFormRelationshipRule(policy.Rule):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        instance: model.application.ApplicationInstance = context.resource

        for form in instance.forms:
            if set(user.groups) & set(form.groups):
                return True
        return False


class ApplicationFormPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return isinstance(context.resource, model.interface.Form)

    def condition(self, context: AttributeContext) -> bool:
        return True


class ApplicationFormViewPolicy(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'view'

    def condition(self, context: AttributeContext) -> bool:
        return True


class ViewFormRule(policy.Rule):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        form: model.interface.Form = context.resource

        if set(user.groups) & set(form.groups):
            return True
        return False


class JobPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return isinstance(context.resource, model.job.Job)

    def condition(self, context: AttributeContext) -> bool:
        return True


class JobViewPolicy(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'view'

    def condition(self, context: AttributeContext) -> bool:
        return True


class ViewJobUsingFormRelationshipRule(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        job: model.job.Job = context.resource

        if set(user.groups) & set(job.form.groups):
            return True
        return False


class JobExecutionPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return isinstance(context.resource, model.job.JobExecution)

    def condition(self, context: AttributeContext) -> bool:
        return True


class JobExecutionViewPolicy(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'view'

    def condition(self, context: AttributeContext) -> bool:
        return True


class ViewJobExecutionUsingFormRelationshipRule(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        execution: model.job.JobExecution = context.resource

        if set(user.groups) & set(execution.job.form.groups):
            return True
        return False


class JobExecutionArtifactPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return isinstance(context.resource, model.job.JobExecutionArtifact)

    def condition(self, context: AttributeContext) -> bool:
        return True


class JobExecutionArtifactViewPolicy(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'view'

    def condition(self, context: AttributeContext) -> bool:
        return True


class ViewJobExecutionArtifactUsingFormRelationshipRule(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        execution: model.job.JobExecutionArtifact = context.resource

        if set(user.groups) & set(execution.job_execution.job.form.groups):
            return True
        return False


default_policies = policy.AuthorizationEngine(
    'default',
    RootPolicySet(
        UserPolicySet(
            ApplicationPolicySet(
                ApplicationViewPolicy(
                    ViewApplicationUsingFormRelationshipRule()
                ),
                GenericCreatePolicy(
                    RejectCreateRule()
                )
            ),
            ApplicationInstancePolicySet(
                ApplicationInstanceViewPolicy(
                    ViewApplicationInstanceUsingFormRelationshipRule()
                ),
                GenericCreatePolicy(
                    RejectCreateRule()
                )
            ),
            ApplicationFormPolicySet(
                ApplicationFormViewPolicy(
                    ViewFormRule()
                ),
                GenericCreatePolicy(
                    RejectCreateRule()
                )
            ),
            JobPolicySet(
                JobViewPolicy(
                    ViewJobUsingFormRelationshipRule()
                )
            ),
            JobExecutionPolicySet(
                JobExecutionViewPolicy(
                    ViewJobExecutionUsingFormRelationshipRule()
                )
            ),
            JobExecutionArtifactPolicySet(
                JobExecutionArtifactViewPolicy(
                    ViewJobExecutionArtifactUsingFormRelationshipRule()
                )
            )
        )
    )
)
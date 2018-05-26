"""
# ---------------------------
# Applications
# ---------------------------
  expansion: (interfaces, instances, instances.schedules, instances.heartbeat)
# ---------------------------
/applications/
/applications/:id
/applications/:id/interfaces
/applications/:id/instances


# ---------------------------
# Application Instances
# ---------------------------
  expansion: (application, schedules, heartbeat, instanceInterfaces, instanceInterfaces.groups, instanceInterfaces.interface)
# ---------------------------
/application-instances/
/application-instances/:id
/application-instances/:id/schedules
/application-instances/:id/heartbeat
/application-instances/:id/instance-interfaces


# ---------------------------
# Application Interfaces
# ---------------------------
  expansion: (inputValues, inputFiles, instanceInterfaces)
# ---------------------------
/application-interfaces/:id
/application-interfaces/:id/instance-interfaces


# ---------------------------
# Instance Interfaces
# ---------------------------
  expansion: (instance, interface, groups, schedules, interface.values, interface.files)
# ---------------------------
/instance-interfaces/:id
/instance-interfaces/:id/jobs [pagination]
/instance-interfaces/:id/jobs/:id
/instance-interfaces/:id/schedules
/instance-interfaces/:id/groups


# ---------------------------
# Jobs
# ---------------------------
  expansion: (inputs, schedules, instanceInterface)
# ---------------------------
/job/ [pagination, user's executions, status-filters]
/jobs/:id
/jobs/:id/schedules
/jobs/:id/inputs
/jobs/:id/job-executions [pagination]
/jobs/:id/job-executions/:id


# ---------------------------
# Job Executions
# ---------------------------
  expansion: ()
# ---------------------------
/job-executions/ [pagination, user's executions, status-filters]
/job-executions/:id
/job-executions/:id/artifacts [pagination, sort, filters, groupings, counts]
/job-executions/:id/job-execution-streams [pagination, sort, filters, groupings, counts]


# ---------------------------
# Job Execution Streams
# ---------------------------
  expansion: ()
# ---------------------------
/job-execution-streams/:id
/job-execution-streams/:id/artifacts [pagination, sort, filters, groupings, counts]

# ---------------------------
# Job Execution Artifacts
# ---------------------------
  expansion: ()
# ---------------------------
/job-execution-artifacts/:id

"""


from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_restplus import Api

from riberry.policy import AttributeContext
from riberry.rest import views
from riberry import model, policy
from riberry.plugins.default.auth import hash_password


model.init(url='sqlite:///model.db', echo=False)

# aii: model.interface.ApplicationInstanceInterface = model.interface.ApplicationInstanceInterface.query().filter_by(id=1).one()
# for val in aii.interface.input_value_definitions:
#     print(val, val.internal_name, val.allowed_values)
# for fil in aii.interface.input_file_definitions:
#     print(fil, fil.internal_name)
#
# print(aii.interface.input_file_definitions)
# print(aii)
#
# x = {
#     'values': {
#         'property_value': 'JKL',
#     },
#     'files': {
#         'config': b'...'
#     }
# }
#
# exit()

# from riberry.model.group import Group
#
# for group in Group.query().all():
#     print(group)
#     print(group.user_associations)
#     print(group.users)
#     print(group.instance_interface_associations)
#     print()
#
# exit()

def preload():
    user = model.auth.User(username='admin', password=hash_password(b'123').decode())
    model.conn.add(user)
    user = model.auth.User(username='shadyrafehi', password=hash_password(b'123').decode())
    model.conn.add(user)
    model.conn.commit()
    # group = model.group.Group(name='Sample Group')
    # model.conn.add(
    #     model.group.ResourceGroupAssociation(
    #         resource_id=user.id,
    #         resource_type=model.group.ResourceType.user,
    #         group=group
    #     )
    # )
    #
    # model.conn.commit()
    #
    # app_instance = model.application.ApplicationInstance(
    #     name='Sample Application Instance',
    #     internal_name='application.sample.instance'
    # )
    # app_interface = model.interface.ApplicationInterface(
    #     name='Sample Interface',
    #     internal_name='interface.sample',
    #     version=1,
    #     input_file_definitions=[
    #         model.interface.InputFileDefinition(
    #             name='CSV File',
    #             internal_name='csv_file',
    #             type='csv',
    #             required=True
    #         )
    #     ],
    #     input_value_definitions=[
    #         model.interface.InputValueDefinition(
    #             name='Application Timeout',
    #             internal_name='timeout',
    #             type='number',
    #             required=True,
    #             default_binary=b'60'
    #         ),
    #         model.interface.InputValueDefinition(
    #             name='Target',
    #             internal_name='target',
    #             type='text',
    #             required=True,
    #             default_binary=b'A',
    #             allowed_binaries=[
    #                 b'A',
    #                 b'B',
    #                 b'C'
    #             ]
    #         )
    #     ]
    # )
    #
    # app = model.application.Application(
    #     name='Sample Application',
    #     internal_name='application.sample',
    #     type='MISC',
    #     instances=[
    #         app_instance
    #     ],
    #     interfaces=[
    #         app_interface
    #     ]
    # )
    #
    # from datetime import time
    #
    # instance_interface = model.interface.ApplicationInstanceInterface(
    #     instance=app_instance,
    #     interface=app_interface,
    #     schedules=[
    #         model.interface.ApplicationInstanceInterfaceSchedule(
    #             start=time(hour=2),
    #             end=time(hour=6)
    #         )
    #     ]
    # )
    #
    # model.conn.add(app)
    # model.conn.add(instance_interface)
    # model.conn.commit()
    #
    # gra = model.group.ResourceGroupAssociation(
    #     group=group,
    #     resource_id=instance_interface.id,
    #     resource_type=model.group.ResourceType.application_instance_interface,
    # )
    #
    # model.conn.add(gra)
    # model.conn.commit()
    #
    # user = model.auth.User(username='johndoe', password=hash_password(b'123').decode())
    # model.conn.add(user)
    # model.conn.commit()
    # group = model.group.Group(name='Another Group')
    # model.conn.add(
    #     model.group.ResourceGroupAssociation(
    #         resource_id=user.id,
    #         resource_type=model.group.ResourceType.user,
    #         group=group
    #     )
    # )
    # model.conn.commit()


if not model.auth.User.query().first():
    preload()


authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'Authorization'
    }
}


app = Flask(__name__)
app.config.SWAGGER_UI_JSONEDITOR = True
CORS(app)
api: Api = Api(app, authorizations=authorizations, security='session')
api.add_namespace(views.application.api)
api.add_namespace(views.application_instance.api)
api.add_namespace(views.form.api)
api.add_namespace(views.application_interface.api)
api.add_namespace(views.auth.api)
api.add_namespace(views.job.api)


# region Authorization


class RootPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        return context.subject is not None


class AdminPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.subject.username == 'admin'

    def condition(self, context: AttributeContext) -> bool:
        return True


class UserPolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.subject.username != 'admin'

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


class ViewApplicationUsingInstanceInterfaceRelationshipRule(policy.Rule):

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


class ViewApplicationInstanceUsingInstanceInterfaceRelationshipRule(policy.Rule):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        instance: model.application.ApplicationInstance = context.resource

        for form in instance.forms:
            if set(user.groups) & set(form.groups):
                return True
        return False


class ApplicationInterfacePolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return isinstance(context.resource, model.interface.ApplicationInterface)

    def condition(self, context: AttributeContext) -> bool:
        return True


class ApplicationInterfaceViewPolicy(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'view'

    def condition(self, context: AttributeContext) -> bool:
        return True


class ViewApplicationInstanceUsingInterfaceRelationshipRule(policy.Rule):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        application_interface: model.interface.ApplicationInterface = context.resource

        for form in application_interface.forms:
            if set(user.groups) & set(form.groups):
                return True
        return False


class ApplicationInstanceInterfacePolicySet(policy.PolicySet):

    def target_clause(self, context: AttributeContext) -> bool:
        return isinstance(context.resource, model.interface.Form)

    def condition(self, context: AttributeContext) -> bool:
        return True


class ApplicationInstanceInterfaceViewPolicy(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return context.action == 'view'

    def condition(self, context: AttributeContext) -> bool:
        return True


class ViewInstanceInterfaceRule(policy.Rule):

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


class ViewJobUsingInstanceInterfaceRelationshipRule(policy.Policy):

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        user: model.auth.User = context.subject
        job: model.job.Job = context.resource

        job.instance


# endregion

auth_engine = policy.AuthorizationEngine(
    # RootPolicySet(
    #     AdminPolicySet(),
    #     UserPolicySet(
    #         ApplicationPolicySet(
    #             ApplicationViewPolicy(
    #                 ViewApplicationUsingInstanceInterfaceRelationshipRule()
    #             ),
    #             GenericCreatePolicy(
    #                 RejectCreateRule()
    #             )
    #         ),
    #         ApplicationInstancePolicySet(
    #             ApplicationInstanceViewPolicy(
    #                 ViewApplicationInstanceUsingInstanceInterfaceRelationshipRule()
    #             ),
    #             GenericCreatePolicy(
    #                 RejectCreateRule()
    #             )
    #         ),
    #         ApplicationInterfacePolicySet(
    #             ApplicationInterfacePolicySet(
    #                 ViewApplicationInstanceUsingInterfaceRelationshipRule()
    #             ),
    #             GenericCreatePolicy(
    #                 RejectCreateRule()
    #             )
    #         ),
    #         ApplicationInstanceInterfacePolicySet(
    #             ApplicationInstanceInterfaceViewPolicy(
    #                 ViewInstanceInterfaceRule()
    #             ),
    #             GenericCreatePolicy(
    #                 RejectCreateRule()
    #             )
    #         ),
    #         JobPolicySet(
    #             JobViewPolicy(
    #                 ViewJobUsingInstanceInterfaceRelationshipRule()
    #             )
    #         )
    #     )
    # )
)


@api.errorhandler(PermissionError)
def permission_error_handler(error):
    code = getattr(error, 'code', 403)
    return {
        'error': True,
        'message': 'You do have have the correct permissions to access this resource',
        'code': code
    }, code


@app.before_request
def set_auth_context():
    user = None
    if 'Authorization' in request.headers:
        auth_header = request.headers['Authorization']
        schema, token = auth_header.split(' ')
        if schema == 'Bearer':
            try:
                payload = model.auth.AuthToken.verify(token)
                user = model.auth.User.query().filter_by(username=payload['subject']).one()
            except Exception as exc:
                print(exc)
                raise

    print(user)
    policy.context.configure(
        subject=user,
        environment=None,
        policy_engine=auth_engine
    )


@app.teardown_request
def rem_auth_context(*_):
    policy.context.reset()


if __name__ == '__main__':
    app.run(debug=True)

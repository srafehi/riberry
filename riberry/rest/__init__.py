import os
from flask import Flask, request, Blueprint, send_from_directory
from flask_cors import CORS
from flask_restplus import Api

from riberry import model, policy, config
from riberry.policy import AttributeContext
from riberry.rest import views


DIRECTORY = os.environ.get('RIBERRY_WEBAPP_PATH', None)

model.init(url=config.config.database.connection_string, echo=config.config.database.echo)


def preload():
    user = model.auth.User(
        username='admin',
        password=model.auth.User.secure_password('admin').decode(),
        auth_provider='default',
        details=model.auth.UserDetails(
            first_name='Admin',
            last_name='Admin',
            display_name='System Administrator',
            department='Operations',
            email='admin@email.com'
        )
    )
    model.conn.add(user)
    model.conn.commit()


if not model.auth.User.query().first():
    preload()

authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'Authorization'
    }
}

app = Flask(__name__, static_url_path='/_')
app.config.SWAGGER_UI_JSONEDITOR = True
CORS(app)

blueprint: Blueprint = Blueprint('api', __name__, url_prefix='/api/0')
api: Api = Api(blueprint, authorizations=authorizations, security='session')
api.add_namespace(views.application.api)
api.add_namespace(views.application_instance.api)
api.add_namespace(views.form.api)
api.add_namespace(views.application_interface.api)
api.add_namespace(views.auth.api)
api.add_namespace(views.job.api)
api.add_namespace(views.self.api)
app.register_blueprint(blueprint)


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_frontend(path):
    full_path = os.path.realpath(os.path.join(DIRECTORY, path or 'index.html'))
    if path and os.path.exists(full_path):
        directory, filename = os.path.split(full_path)
        return send_from_directory(directory, filename)
    else:
        return send_from_directory(DIRECTORY, 'index.html')



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


# endregion

auth_engine = policy.AuthorizationEngine(
    RootPolicySet(
        AdminPolicySet(),
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
            ApplicationInterfacePolicySet(
                ApplicationInterfacePolicySet(
                    ViewApplicationInstanceUsingInterfaceRelationshipRule()
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
            )
        )
    )
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
                raise

    policy.context.configure(
        subject=user,
        environment=None,
        policy_engine=auth_engine
    )


@app.teardown_request
def rem_auth_context(*_):
    policy.context.reset()
    model.conn.close()


if __name__ == '__main__':
    # import waitress
    # waitress.serve(app, port=5000)
    app.run(debug=True)

from flask import request
from flask_restplus import Namespace, Resource

from riberry.rest import services
from . import args

api = Namespace('auth', description='Authentication and authorization')


@api.route('/token', endpoint='auth-token:resource')
class AccessTokenResource(Resource):

    def post(self):
        data = request.json
        username, password = data.get('username'), data.get('password')
        return services.auth.authenticate_user(username=username, password=password)


@api.route('/groups', endpoint='auth-group:collection')
class GroupCollectionResource(Resource):

    def get(self):
        return services.auth.all_groups()

    def post(self):
        data = request.json
        return services.auth.create_group(
            name=data['name']
        )


@api.route('/groups/<id>', endpoint='auth-group:resource')
class GroupInstanceResource(Resource):

    def get(self, id):
        return services.auth.group_by_id(group_id=id)


@api.route('/groups/<id>/users', endpoint='auth-group:user:collection')
class GroupInstanceUserCollectionResource(Resource):

    def get(self, id):
        return services.auth.users_for_group_id(group_id=id)


@api.route('/groups/<id>/forms', endpoint='auth-group:form:collection')
class GroupFormCollectionResource(Resource):

    def get(self, id):
        return services.auth.forms_for_group_id(group_id=id)


@api.route('/groups/<id>/users/<user_id>', endpoint='auth-group:user:resource')
class GroupUserCollectionResource(Resource):

    def post(self, id, user_id):
        return services.auth.add_user_to_group(group_id=id, user_id=user_id)

    def delete(self, id, user_id):
        return services.auth.remove_user_from_group(group_id=id, user_id=user_id)


@api.route('/groups/<id>/forms/<form_id>', endpoint='auth-group:form:resource')
class GroupFormResource(Resource):

    def post(self, id, form_id):
        return services.auth.add_form_to_group(group_id=id, form_id=form_id)

    def delete(self, id, form_id):
        return services.auth.remove_form_from_group(group_id=id, form_id=form_id)

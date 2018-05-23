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


@api.route('/groups/<id>/instance-interfaces', endpoint='auth-group:instance-interface:collection')
class GroupInstanceInstanceInterfaceCollectionResource(Resource):

    def get(self, id):
        return services.auth.instance_interfaces_for_group_id(group_id=id)


@api.route('/groups/<id>/users/<user_id>', endpoint='auth-group:user:resource')
class GroupInstanceUserCollectionResource(Resource):

    def post(self, id, user_id):
        return services.auth.add_user_from_group(group_id=id, user_id=user_id)

    def delete(self, id, user_id):
        return services.auth.remove_user_from_group(group_id=id, user_id=user_id)


@api.route('/groups/<id>/instance-interfaces/<instance_interface_id>', endpoint='auth-group:instance-interface:resource')
class GroupInstanceInstanceInterfaceCollectionResource(Resource):

    def post(self, id, instance_interface_id):
        return services.auth.add_instance_interface_from_group(group_id=id, instance_interface_id=instance_interface_id)

    def delete(self, id, instance_interface_id):
        return services.auth.remove_instance_interface_from_group(group_id=id, instance_interface_id=instance_interface_id)

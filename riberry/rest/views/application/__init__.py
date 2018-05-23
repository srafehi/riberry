from flask import request
from flask_restplus import Namespace, Resource
from webargs.flaskparser import use_args

from riberry.rest import services, views
from . import args
from ..base import parse_args

api = Namespace('applications', description='Application resources')


@api.route('/', endpoint='application:collection')
class ApplicationCollectionResource(Resource):

    @use_args(args.base)
    def get(self, options):
        options = parse_args(options)
        return views.make_response(data=services.application.all_applications(options))

    def post(self):
        data = request.get_json()
        resp = services.application.create_application(
            name=data['name'],
            internal_name=data['internalName'],
            type=data['type'],
            description=data.get('description')
        )

        return views.make_response(data=resp)


@api.route('/<id>', endpoint='application:resource')
class ApplicationResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.application.application_by_id(application_id=id, options=options))


@api.route('/<id>/instances', endpoint='application:instance:collection')
class ApplicationResourceInstanceCollectionResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.application_instance.instances_by_application_id(application_id=id, options=options))

    def post(self, id):
        data = request.get_json()
        resp = services.application_instance.create_application_instance(
            application_id=id,
            name=data['name'],
            internal_name=data['internalName']
        )

        return views.make_response(data=resp)


@api.route('/<id>/interfaces', endpoint='application:interface:collection')
class ApplicationResourceInterfaceCollectionResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.application_interface.interfaces_by_application_id(application_id=id, options=options))

    def post(self, id):
        data = request.get_json()
        resp = services.application_interface.create_application_interface(
            application_id=id,
            name=data['name'],
            internal_name=data['internalName'],
            version=data['version'],
            description=data.get('description'),
            input_files=data.get('inputFiles', []),
            input_values=data.get('inputValues', [])
        )

        return views.make_response(data=resp)
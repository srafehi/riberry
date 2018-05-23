import json

from flask import request
from flask_restplus import Namespace, Resource
from webargs.flaskparser import use_args

from riberry.rest import services, views
from . import args
from ..base import parse_args

api = Namespace('instance-interfaces', description='Application instance interface resources')


@api.route('/', endpoint='instance-interface:collection')
class ApplicationInstanceInterfaceCollection(Resource):

    @use_args(args.base)
    def get(self, options):
        options = parse_args(options)
        return views.make_response(services.instance_interface.all_instance_interfaces(options=options))

    def post(self):
        data = request.get_json()
        resp = services.instance_interface.create_instance_interface(
            instance_id=data['instanceId'],
            interface_id=data['interfaceId'],
            groups=data.get('groups', [])
        )

        return views.make_response(data=resp)


@api.route('/<id>', endpoint='instance-interface:resource')
class ApplicationInstanceInterfaceResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.instance_interface.instance_interface_by_id(id, options=options))


@api.route('/<id>/jobs', endpoint='instance-interface:job:collection')
class ApplicationInstanceInterfaceJobCollectionResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.job.jobs_by_instance_interface_id(id, options=options))

    def post(self, id):
        input_files = {}
        for key, value in request.files.items():
            input_files[key] = value
        job_name = request.form.get('jobName')
        inputs = json.loads(request.form.get('inputs', '{}'))

        response = services.job.create_job(
            instance_interface_id=id,
            name=job_name,
            input_values=inputs,
            input_files=input_files
        )

        return views.make_response(response)

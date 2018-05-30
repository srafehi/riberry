import json

from flask import request
from flask_restplus import Namespace, Resource
from webargs.flaskparser import use_args

from riberry.rest import services, views
from . import args
from ..base import parse_args

api = Namespace('forms', description='Form resources')


@api.route('/', endpoint='form:collection')
class FormCollection(Resource):

    @use_args(args.base)
    def get(self, options):
        options = parse_args(options)
        return views.make_response(services.form.all_forms(options=options))

    def post(self):
        data = request.get_json()
        resp = services.form.create_form(
            instance_id=data['instanceId'],
            interface_id=data['interfaceId'],
            groups=data.get('groups', [])
        )

        return views.make_response(data=resp)


@api.route('/<id>', endpoint='form:resource')
class FormResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.form.form_by_id(id, options=options))


@api.route('/<id>/jobs', endpoint='form:job:collection')
class FormJobCollectionResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.job.jobs_by_form_id(id, options=options))

    def post(self, id):
        input_files = {}
        for key, value in request.files.items():
            input_files[key] = value
        job_name = request.form.get('jobName')
        execute = request.form.get('executeNow') == '1'
        inputs = json.loads(request.form.get('inputs', '{}'))

        response = services.job.create_job(
            form_id=id,
            name=job_name,
            input_values=inputs,
            input_files=input_files,
            execute=execute
        )

        return views.make_response(response)

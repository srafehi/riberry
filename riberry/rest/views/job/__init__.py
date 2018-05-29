from flask_restplus import Namespace, Resource
from webargs.flaskparser import use_args

from riberry.rest import services, views
from . import args
from ..base import parse_args

api = Namespace('jobs', description='Job resources')


@api.route('/<id>', endpoint='job:resource')
class JobResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.job.job_by_id(job_id=id, options=options))


@api.route('/summary', endpoint='job:summary:collection')
class JobSummaryResource(Resource):

    def get(self):
        return views.make_response(services.job.summary_overall())


@api.route('/<id>/executions', endpoint='job:execution:resource')
class JobExecutionsResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(services.job.job_executions_by_id(job_id=id, options=options))

    def post(self, id):
        return views.make_response(services.job.create_job_execution(job_id=id))

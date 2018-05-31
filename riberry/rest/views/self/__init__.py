from flask_restplus import Namespace, Resource
from webargs.flaskparser import use_args

from riberry.rest import services, views
from . import args
from ..base import parse_args

api = Namespace('self', description='User resources')


@api.route('/', endpoint='my:resource')
class MyResource(Resource):

    @use_args(args.base)
    def get(self, options):
        options = parse_args(options)
        return views.make_response(services.self.profile(options=options))


@api.route('/jobs', endpoint='my:jobs:resource')
class MyJobResource(Resource):

    def get(self):
        return views.make_response(services.job.job_executions_by_id(job_id=id, options=options))
